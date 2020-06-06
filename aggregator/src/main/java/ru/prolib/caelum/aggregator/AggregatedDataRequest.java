package ru.prolib.caelum.aggregator;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.core.Period;

public class AggregatedDataRequest {
	private final String symbol;
	private final Period period;
	private final long from, to, limit;
	
	public AggregatedDataRequest(String symbol, Period period, long from, long to, long limit) {
		this.symbol = symbol;
		this.period = period;
		this.from = from;
		this.to = to;
		this.limit = limit;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public Period getPeriod() {
		return period;
	}
	
	public long getFrom() {
		return from;
	}
	
	public long getTo() {
		return to;
	}
	
	public long getLimit() {
		return limit;
	}
	
	public Instant getTimeFrom() {
		return Instant.ofEpochMilli(from);
	}
	
	public Instant getTimeTo() {
		return Instant.ofEpochMilli(to);
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("period", period)
				.append("from", from)
				.append("to", to)
				.append("limit", limit)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(780011759, 75)
				.append(symbol)
				.append(period)
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
		if ( other == null || other.getClass() != AggregatedDataRequest.class ) {
			return false;
		}
		AggregatedDataRequest o = (AggregatedDataRequest) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.period, period)
				.append(o.from, from)
				.append(o.to, to)
				.append(o.limit, limit)
				.build();
	}
	
}
