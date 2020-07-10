package ru.prolib.caelum.core;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class IteratorStub<T> implements ICloseableIterator<T> {
	protected final List<T> data;
	protected boolean closed = false;
	
	public IteratorStub(List<T> data, boolean make_copy) {
		this.data = make_copy ? new ArrayList<>(data) : data;
	}
	
	public IteratorStub(List<T> data) {
		this(data, false);
	}
	
	public IteratorStub() {
		this(new ArrayList<>());
	}
	
	public boolean closed() {
		return closed;
	}
	
	@Override
	public void close() {
		closed = true;
	}

	@Override
	public boolean hasNext() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		return data.size() > 0;
	}

	@Override
	public T next() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		if ( hasNext() == false ) {
			throw new NoSuchElementException();
		}
		return data.remove(0);
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != IteratorStub.class ) {
			return false;
		}
		IteratorStub<?> o = (IteratorStub<?>) other;
		return new EqualsBuilder()
				.append(o.data, data)
				.append(o.closed, closed)
				.build();
	}

}
