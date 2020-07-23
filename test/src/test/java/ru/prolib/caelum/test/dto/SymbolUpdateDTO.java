package ru.prolib.caelum.test.dto;

import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class SymbolUpdateDTO {
	public Long time;
	public Map<Integer, String> tokens;
	
	public SymbolUpdateDTO() {
		
	}
	
	public SymbolUpdateDTO(long time, Map<Integer, String> tokens) {
		this.time = time;
		this.tokens = tokens;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolUpdateDTO.class ) {
			return false;
		}
		SymbolUpdateDTO o = (SymbolUpdateDTO) other;
		return new EqualsBuilder()
				.append(o.time, time)
				.append(o.tokens, tokens)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(8290117, 79)
				.append(time)
				.append(tokens)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("time", time)
				.append("tokens", tokens)
				.build();
	}
	
}
