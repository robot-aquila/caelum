package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExtensionConf {
	private final String id;
	private final String builderClass;
	private final boolean enabled;
	private final StartupOrder order;
	
	public ExtensionConf(String id, String builderClass, boolean enabled, StartupOrder order) {
		this.id = id;
		this.builderClass = builderClass;
		this.enabled = enabled;
		this.order = order;
	}
	
	public String getId() {
		return id;
	}
	
	public String getBuilderClass() {
		return builderClass;
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public StartupOrder getStartupOrder() {
		return order;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115297, 93)
				.append(id)
				.append(builderClass)
				.append(enabled)
				.append(order)
				.build();
	}
	
	@Override
	public String toString() {
		ToStringBuilder b = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("id", id)
				.append("builder", builderClass)
				.append("enabled", enabled);
		switch ( order.getPriority() ) {
		case FIRST: b.append("order", "first"); break;
		case LAST: b.append("order", "last"); break;
		case NORMAL:
			if ( order.getOrder() == null ) {
				b.append("order", "normal");
			} else {
				b.append("order", order.getOrder());
			}
		}
		return b.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ExtensionConf.class ) {
			return false;
		}
		ExtensionConf o = (ExtensionConf) other;
		return new EqualsBuilder()
				.append(o.id, id)
				.append(o.builderClass, builderClass)
				.append(o.enabled, enabled)
				.append(o.order, order)
				.build();
	}
	
}
