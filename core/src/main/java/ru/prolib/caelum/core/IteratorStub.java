package ru.prolib.caelum.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class IteratorStub<T> implements Iterator<T> {
	private final List<T> data;
	
	public IteratorStub(List<T> data) {
		this.data = data;
	}
	
	public IteratorStub() {
		this(new ArrayList<>());
	}

	@Override
	public boolean hasNext() {
		return data.size() > 0;
	}

	@Override
	public T next() {
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
				.build();
	}

}
