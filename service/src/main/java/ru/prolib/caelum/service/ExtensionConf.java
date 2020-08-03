package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExtensionConf {
	private final String id;
	private final String builderClass;
	private final boolean enabled;
	
	public ExtensionConf(String id, String builderClass, boolean enabled) {
		this.id = id;
		this.builderClass = builderClass;
		this.enabled = enabled;
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
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115297, 93)
				.append(id)
				.append(builderClass)
				.append(enabled)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("id", id)
				.append("builder", builderClass)
				.append("enabled", enabled)
				.build();
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
				.build();
	}
	
}
