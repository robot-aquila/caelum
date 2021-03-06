package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExtensionStatus {
	private final ExtensionState state;
	private final Object statusInfo;
	
	public ExtensionStatus(ExtensionState state, Object statusInfo) {
		this.state = state;
		this.statusInfo = statusInfo;
	}
	
	public ExtensionState getState() {
		return state;
	}
	
	public Object getStatusInfo() {
		return statusInfo;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(414219, 901)
				.append(state)
				.append(statusInfo)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("state", state)
				.append("statusInfo", statusInfo)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ExtensionStatus.class ) {
			return false;
		}
		ExtensionStatus o = (ExtensionStatus) other;
		return new EqualsBuilder()
				.append(o.state, state)
				.append(o.statusInfo, statusInfo)
				.build();
	}
	
}
