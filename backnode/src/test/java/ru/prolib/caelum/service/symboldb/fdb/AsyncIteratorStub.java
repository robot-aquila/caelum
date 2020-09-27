package ru.prolib.caelum.service.symboldb.fdb;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.async.AsyncIterator;

import ru.prolib.caelum.lib.IteratorStub;

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
