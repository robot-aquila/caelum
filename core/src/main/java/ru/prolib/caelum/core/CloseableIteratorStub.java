package ru.prolib.caelum.core;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CloseableIteratorStub<T> extends IteratorStub<T> implements ICloseableIterator<T> {

	public CloseableIteratorStub(List<T> data) {
		super(data);
	}
	
	public CloseableIteratorStub() {
		super();
	}
	
	@Override
	public void close() {
		data.clear();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != CloseableIteratorStub.class ) {
			return false;
		}
		CloseableIteratorStub<?> o = (CloseableIteratorStub<?>) other;
		return new EqualsBuilder()
				.append(o.data, data)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("data", data)
				.build();
	}

}
