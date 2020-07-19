package ru.prolib.caelum.itemdb.kafka;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataResponse;

public class ItemIterator implements IItemIterator {
	private final KafkaConsumer<String, KafkaItem> consumer;
	private final Iterator<ConsumerRecord<String, KafkaItem>> it;
	private final KafkaItemInfo itemInfo;
	private final long limit, endTime;
	private boolean finished = false, closed = false;
	private ConsumerRecord<String, KafkaItem> nextRecord;
	private long recordCount;
	private Long lastOffset;
	
	public ItemIterator(KafkaConsumer<String, KafkaItem> consumer,
			Iterator<ConsumerRecord<String, KafkaItem>> it,
			KafkaItemInfo item_info,
			long limit, long end_time)
	{
		this.consumer = consumer;
		this.it = it;
		this.itemInfo = item_info;
		this.limit = limit;
		this.endTime = end_time;
	}
	
	public KafkaConsumer<String, KafkaItem> getConsumer() {
		return consumer;
	}
	
	public Iterator<ConsumerRecord<String, KafkaItem>> getSourceIterator() {
		return it;
	}
	
	public KafkaItemInfo getItemInfo() {
		return itemInfo;
	}
	
	public long getLimit() {
		return limit;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	private void finish() {
		nextRecord = null;
		finished = true;
	}
	
	public boolean finished() {
		return finished;
	}
	
	public boolean closed() {
		return closed;
	}
	
	public long recordCount() {
		return recordCount;
	}
	
	/**
	 * Transparently advance thru sequence.
	 * <p>
	 * @return false if has no more records
	 * @throws IllegalStateException - partition mismatch
	 * @throws KafkaException - exception thrown by Kafka
	 */
	private boolean advance() throws IllegalStateException {
		if ( finished ) {
			return false;
		}
		if ( itemInfo.hasData() == false ) {
			finish();
			return false;
		}
		
		final String symbol = itemInfo.getSymbol();
		final int partition = itemInfo.getPartition();
		final long end_offset = itemInfo.getEndOffset() - 1;
		for ( ;; ) {
			// Test for limit reached. It may breached by previous record.
			if ( recordCount >= limit ) {
				finish();
				return false;
			}
			if ( it.hasNext() == false ) {
				finish();
				return false;
			}
			// The last offset must be checked prior moving to next record to omit
			// possible lock while reading next record which may be out of available range
			if ( lastOffset != null && lastOffset >= end_offset ) {
				finish();
				return false;
			}
			
			// Get the next record.
			nextRecord = it.next();
			lastOffset = nextRecord.offset();
			// Test for end time
			if ( nextRecord.timestamp() >= endTime ) {
				finish();
				return false;
			}
			// Test for symbol
			if ( symbol.equals(nextRecord.key()) == false ) {
				// Skip this record
				continue;
			}
			// Partition may be changed during our work. Let's check it.
			int record_partition = nextRecord.partition();
			if ( record_partition != partition ) {
				finish();
				throw new IllegalStateException(new StringBuilder()
						.append("Partition changed: expected=")
						.append(partition)
						.append(" actual=")
						.append(record_partition)
						.toString());
			}
			// It's our record
			recordCount ++;
			return true;
		}
	}

	@Override
	public boolean hasNext() {
		if ( finished || closed ) {
			return false;
		}
		if ( nextRecord != null ) {
			return true;
		}
		return advance();
	}

	@Override
	public IItem next() {
		if ( finished || closed ) {
			throw new NoSuchElementException();
		}
		if ( nextRecord == null && ! advance() ) {
			throw new NoSuchElementException();
		}
		Item data = new Item(itemInfo.getSymbol(), nextRecord.timestamp(),
				nextRecord.offset(), nextRecord.value());
		advance();
		return data;
	}

	@Override
	public ItemDataResponse getMetaData() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		return new ItemDataResponse(lastOffset == null ? 0 : lastOffset, DigestUtils.md5Hex(new StringBuilder()
				.append(itemInfo.getSymbol())
				.append(":")
				.append(itemInfo.getStartOffset())
				.append(":")
				.append(itemInfo.getNumPartitions())
				.toString())
			);
	}
	
	@Override
	public void close() {
		if ( closed == false ) {
			finish();
			closed = true;
			consumer.close();
		}
	}

}
