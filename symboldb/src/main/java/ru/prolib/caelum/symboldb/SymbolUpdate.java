package ru.prolib.caelum.symboldb;

import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SymbolUpdate {
	private final String symbol;
	private final long time;
	private final Map<Integer, String> tokens;
	
	public SymbolUpdate(String symbol, long time, Map<Integer, String> tokens) {
		this.symbol = symbol;
		this.time = time;
		this.tokens = tokens;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public long getTime() {
		return time;
	}
	
	public Map<Integer, String> getTokens() {
		return tokens;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("tokens", tokens)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(107789, 405)
				.append(symbol)
				.append(time)
				.append(tokens)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolUpdate.class ) {
			return false;
		}
		SymbolUpdate o = (SymbolUpdate) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.time, time)
				.append(o.tokens, tokens)
				.build();
	}
	
}
