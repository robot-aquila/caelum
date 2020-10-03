package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class StartupOrder {
	private final StartupPriority priority;
	private final Integer order;
	
	public StartupOrder(StartupPriority priority, Integer order) {
		this.priority = priority;
		this.order = order;
	}
	
	public StartupPriority getPriority() {
		return priority;
	}
	
	public Integer getOrder() {
		return order;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(888164015, 3709)
				.append(priority)
				.append(order)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != StartupOrder.class ) {
			return false;
		}
		StartupOrder o = (StartupOrder) other;
		return new EqualsBuilder()
				.append(o.priority, priority)
				.append(o.order, order)
				.build();
	}

}
