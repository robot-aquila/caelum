package ru.prolib.caelum.service;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ItemDataRequestContinue implements IItemDataRequest {
	private final String symbol;
	private final long offset;
	private final String magic;
	private final Long to;
	private final Integer limit;
	
	public ItemDataRequestContinue(String symbol, long offset, String magic, Long to, Integer limit) {
		this.symbol = symbol;
		this.offset = offset;
		this.magic = magic;
		this.to = to;
		this.limit = limit;
	}
	
	@Override
	public String getSymbol() {
		return symbol;
	}
	
	public long getOffset() {
		return offset;
	}
	
	public String getMagic() {
		return magic;
	}
	
	@Override
	public Long getTo() {
		return to;
	}
	
	@Override
	public Integer getLimit() {
		return limit;
	}
	
	@Override
	public Instant getTimeTo() {
		return to == null ? null : Instant.ofEpochMilli(to);
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("offset", offset)
				.append("magic", magic)
				.append("to", to)
				.append("limit", limit)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(10009827, 15)
				.append(symbol)
				.append(offset)
				.append(magic)
				.append(to)
				.append(limit)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ItemDataRequestContinue.class ) {
			return false;
		}
		ItemDataRequestContinue o = (ItemDataRequestContinue) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.offset, offset)
				.append(o.magic, magic)
				.append(o.to, to)
				.append(o.limit, limit)
				.build();
	}
	
}
