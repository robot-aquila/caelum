package ru.prolib.caelum.aggregator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.core.Period;

public class AggregatorStatus {
	private final Period period;
	private final AggregatorType type;
	private final AggregatorState state;
	private final Object statusInfo;
	
	public AggregatorStatus(Period period,
			AggregatorType type,
			AggregatorState state,
			Object statusInfo)
	{
		this.period = period;
		this.type = type;
		this.state = state;
		this.statusInfo = statusInfo;
	}
	
	public Period getPeriod() {
		return period;
	}
	
	public AggregatorType getType() {
		return type;
	}
	
	public AggregatorState getState() {
		return state;
	}
	
	public Object getStatusInfo() {
		return statusInfo;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("period", period)
				.append("type", type)
				.append("state", state)
				.append("statusInfo", statusInfo)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(815347, 117)
				.append(period)
				.append(type)
				.append(state)
				.append(statusInfo)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != AggregatorStatus.class ) {
			return false;
		}
		AggregatorStatus o = (AggregatorStatus) other;
		return new EqualsBuilder()
				.append(o.period, period)
				.append(o.type, type)
				.append(o.state, state)
				.append(o.statusInfo, statusInfo)
				.build();
	}
	
}
