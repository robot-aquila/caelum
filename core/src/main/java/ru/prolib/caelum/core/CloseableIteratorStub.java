package ru.prolib.caelum.core;

import java.util.List;

public class CloseableIteratorStub<T> extends IteratorStub<T> implements ICloseableIterator<T> {

	public CloseableIteratorStub(List<T> data) {
		super(data);
	}
	
	@Override
	public void close() {
		
	}

}
