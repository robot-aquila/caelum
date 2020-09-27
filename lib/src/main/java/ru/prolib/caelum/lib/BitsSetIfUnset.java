package ru.prolib.caelum.lib;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class BitsSetIfUnset implements ConditionalBitwiseOperator {
	private final int cond;
	private boolean applied = false;
	
	public BitsSetIfUnset(int cond) {
		this.cond = cond;
	}
	
	@Override
	public boolean applied() {
		return applied;
	}

	@Override
	public int applyAsInt(int state, int arg) {
		return (applied = ((state & cond) == 0)) ? (state | arg) : state;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115009, 901)
				.append(cond)
				.append(applied)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != BitsSetIfUnset.class ) {
			return false;
		}
		BitsSetIfUnset o = (BitsSetIfUnset) other;
		return new EqualsBuilder()
				.append(o.cond, cond)
				.append(o.applied, applied)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("cond", cond)
				.append("applied", applied)
				.build();
	}
	
}