package ru.prolib.caelum.service.symboldb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SymbolListRequest {
	private final String category;
	private final String afterSymbol;
	private final Integer limit;
	
	public SymbolListRequest(String category, String afterSymbol, Integer limit) {
		this.category = category;
		this.afterSymbol = afterSymbol;
		this.limit = limit;
	}
	
	public SymbolListRequest(String category, Integer limit) {
		this(category, null, limit);
	}
	
	public String getCategory() {
		return category;
	}
	
	public String getAfterSymbol() {
		return afterSymbol;
	}
	
	public Integer getLimit() {
		return limit;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(221610091, 103)
				.append(category)
				.append(afterSymbol)
				.append(limit)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("category", category)
				.append("afterSymbol", afterSymbol)
				.append("limit", limit)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolListRequest.class ) {
			return false;
		}
		SymbolListRequest o = (SymbolListRequest) other;
		return new EqualsBuilder()
				.append(o.category, category)
				.append(o.afterSymbol, afterSymbol)
				.append(o.limit, limit)
				.build();
	}
	
}
