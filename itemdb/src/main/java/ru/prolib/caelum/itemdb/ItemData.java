package ru.prolib.caelum.itemdb;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import ru.prolib.caelum.core.Item;

public class ItemData implements IItemData {
	private final String symbol;
	private final long time, offset;
	private final Item item;
	
	public ItemData(String symbol, long time, long offset, Item item) {
		this.symbol = symbol;
		this.time = time;
		this.offset = offset;
		this.item = item;
	}
	
	public ItemData(ConsumerRecord<String, Item> record) {
		this(record.key(), record.timestamp(), record.offset(), record.value());
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
	public Item getItem() {
		return item;
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
		if ( other == null || (other instanceof IItemData) == false ) {
			return false;
		}
		IItemData o = (IItemData) other;
		return new EqualsBuilder()
				.append(o.getSymbol(), symbol)
				.append(o.getTime(), time)
				.append(o.getOffset(), offset)
				.append(o.getItem(), item)
				.build();
	}

	@Override
	public Instant getTimeAsInstant() {
		return Instant.ofEpochMilli(time);
	}
}
