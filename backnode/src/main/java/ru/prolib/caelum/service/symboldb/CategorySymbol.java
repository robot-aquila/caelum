package ru.prolib.caelum.service.symboldb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CategorySymbol {
	private final String category, symbol;
	
	public CategorySymbol(String category, String symbol) {
		this.category = category;
		this.symbol = symbol;
	}
	
	public String getCategory() {
		return category;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(90014257, 19)
				.append(category)
				.append(symbol)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("category", category)
				.append("symbol", symbol)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != CategorySymbol.class ) {
			return false;
		}
		CategorySymbol o = (CategorySymbol) other;
		return new EqualsBuilder()
				.append(o.category, category)
				.append(o.symbol, symbol)
				.build();
	}

}
