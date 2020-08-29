package ru.prolib.caelum.itemdb.kafka;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.feeder.ak.KafkaItem;

public class Item implements IItem {
	private final String symbol;
	private final long time, offset;
	private final KafkaItem item;
	
	public Item(String symbol, long time, long offset, KafkaItem item) {
		this.symbol = symbol;
		this.time = time;
		this.offset = offset;
		this.item = item;
	}
	
	public Item(ConsumerRecord<String, KafkaItem> record) {
		this(record.key(), record.timestamp(), record.offset(), record.value());
	}
	
	public KafkaItem getKafkaItem() {
		return item;
	}

	@Override
	public String getSymbol() {
		return symbol;
	}

	@Override
	public long getTime() {
		return time;
	}
	
	@Override
	public long getOffset() {
		return offset;
	}

	@Override
	public Instant getTimeAsInstant() {
		return Instant.ofEpochMilli(time);
	}

	@Override
	public ItemType getType() {
		return item.getType();
	}

	@Override
	public long getValue() {
		return item.getValue();
	}

	@Override
	public long getVolume() {
		return item.getVolume();
	}

	@Override
	public byte getDecimals() {
		return item.getDecimals();
	}

	@Override
	public byte getVolumeDecimals() {
		return item.getVolumeDecimals();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("offset", offset)
				.append("item", item)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(4257009, 31)
				.append(symbol)
				.append(time)
				.append(offset)
				.append(item)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other instanceof IItem == false ) {
			return false;
		}
		IItem o = (IItem) other;
		return new EqualsBuilder()
				.append(o.getSymbol(), getSymbol())
				.append(o.getTime(), getTime())
				.append(o.getOffset(), getOffset())
				.append(o.getValue(), getValue())
				.append(o.getDecimals(), getDecimals())
				.append(o.getVolume(), o.getVolume())
				.append(o.getVolumeDecimals(), getVolumeDecimals())
				.append(o.getType(), getType())
				.build();
	}

}
