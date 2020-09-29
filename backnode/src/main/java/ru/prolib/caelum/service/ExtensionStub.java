package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class ExtensionStub implements IExtension {
	private final ExtensionStatus status;
	
	public ExtensionStub(ExtensionStatus status) {
		this.status = status;
	}
	
	public ExtensionStatus getStatus() {
		return status;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ExtensionStub.class ) {
			return false;
		}
		ExtensionStub o = (ExtensionStub) other;
		return new EqualsBuilder()
				.append(o.status, status)
				.build();
	}

}
