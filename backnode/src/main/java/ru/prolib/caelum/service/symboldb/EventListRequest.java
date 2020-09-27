package ru.prolib.caelum.service.symboldb;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class EventListRequest {
	private final String symbol;
	private final Long from, to;
	private final Integer limit;
	
	public EventListRequest(String symbol, Long from, Long to, Integer limit) {
		this.symbol = symbol;
		this.from = from;
		this.to = to;
		this.limit = limit;
	}
	
	public EventListRequest(String symbol) {
		this(symbol, null, null, null);
	}
	
	public boolean isValid() {
		return symbol != null && symbol.length() > 0;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public Long getFrom() {
		return from;
	}
	
	public Long getTo() {
		return to;
	}
	
	public Integer getLimit() {
		return limit;
	}
	
	public Instant getTimeFrom() {
		return from == null ? null : Instant.ofEpochMilli(from);
	}
	
	public Instant getTimeTo() {
		return to == null ? null : Instant.ofEpochMilli(to);
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
		return new HashCodeBuilder(5370241, 47)
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
		if ( other == null || other.getClass() != EventListRequest.class ) {
			return false;
		}
		EventListRequest o = (EventListRequest) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.from, from)
				.append(o.to, to)
				.append(o.limit, limit)
				.build();
	}

}
