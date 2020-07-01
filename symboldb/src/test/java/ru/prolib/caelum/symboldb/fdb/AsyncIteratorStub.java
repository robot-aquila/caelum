package ru.prolib.caelum.symboldb.fdb;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.async.AsyncIterator;

import ru.prolib.caelum.core.IteratorStub;

public class AsyncIteratorStub<T> extends IteratorStub<T> implements AsyncIterator<T> {
	
	public AsyncIteratorStub(List<T> data) {
		super(data);
	}
	
	public AsyncIteratorStub() {
		super();
	}

	@Override
	public CompletableFuture<Boolean> onHasNext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cancel() {
		throw new UnsupportedOperationException();
	}

}
