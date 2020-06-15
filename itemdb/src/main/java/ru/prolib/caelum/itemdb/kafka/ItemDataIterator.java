package ru.prolib.caelum.itemdb.kafka;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.itemdb.IItemData;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.ItemData;
import ru.prolib.caelum.itemdb.ItemDataResponse;

public class ItemDataIterator implements IItemDataIterator {
	private final KafkaConsumer<String, Item> consumer;
	private final Iterator<ConsumerRecord<String, Item>> it;
	private final ItemInfo itemInfo;
	private final long limit;
	private boolean finished = false, closed = false;
	private ConsumerRecord<String, Item> nextRecord;
	private long recordCount, lastOffset;
	
	public ItemDataIterator(KafkaConsumer<String, Item> consumer,
			Iterator<ConsumerRecord<String, Item>> it,
			ItemInfo item_info,
			long limit)
	{
		this.consumer = consumer;
		this.it = it;
		this.itemInfo = item_info;
		this.limit = limit;
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
		final long end_offset = itemInfo.getEndOffset();
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
			// Get the next record.
			nextRecord = it.next();
			lastOffset = nextRecord.offset();
			// Test for endOffset reached
			if ( nextRecord.offset() > end_offset ) {
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
	public IItemData next() {
		if ( finished || closed ) {
			throw new NoSuchElementException();
		}
		if ( nextRecord == null && ! advance() ) {
			throw new NoSuchElementException();
		}
		ItemData data = new ItemData(itemInfo.getSymbol(), nextRecord.timestamp(), nextRecord.value());
		advance();
		return data;
	}

	@Override
	public ItemDataResponse getMetaData() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		//itemInfo.getStartOffset();
		//itemInfo.getNumPartitions();
		return new ItemDataResponse(lastOffset + 1, "TODO");
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
