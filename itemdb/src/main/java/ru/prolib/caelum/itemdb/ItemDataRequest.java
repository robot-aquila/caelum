package ru.prolib.caelum.itemdb;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ItemDataRequest implements IItemDataRequest {
	private final String symbol;
	private final long from, to, limit;
	
	public ItemDataRequest(String symbol, long from, long to, long limit) {
		this.symbol = symbol;
		this.from = from;
		this.to = to;
		this.limit = limit;
	}
	
	@Override
	public String getSymbol() {
		return symbol;
	}
	
	public long getFrom() {
		return from;
	}
	
	@Override
	public long getTo() {
		return to;
	}
	
	@Override
	public long getLimit() {
		return limit;
	}
	
	public Instant getTimeFrom() {
		return Instant.ofEpochMilli(from);
	}
	
	@Override
	public Instant getTimeTo() {
		return Instant.ofEpochMilli(to);
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("from", from)
				.append("to", to)
				.append("limit", limit)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(99766117, 93)
				.append(symbol)
				.append(from)
				.append(to)
				.append(limit)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ItemDataRequest.class ) {
			return false;
		}
		ItemDataRequest o = (ItemDataRequest) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.from, from)
				.append(o.to, to)
				.append(o.limit, limit)
				.build();
	}
	
}
