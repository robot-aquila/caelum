package ru.prolib.caelum.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

}
