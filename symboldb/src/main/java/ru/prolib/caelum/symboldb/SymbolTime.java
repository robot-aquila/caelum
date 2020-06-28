package ru.prolib.caelum.symboldb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SymbolTime {
	private final String symbol;
	private final long time;
	
	public SymbolTime(String symbol, long time) {
		this.symbol = symbol;
		this.time = time;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public long getTime() {
		return time;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(100235, 91)
				.append(symbol)
				.append(time)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolTime.class ) {
			return false;
		}
		SymbolTime o = (SymbolTime) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.time, time)
				.build();
	}

}
