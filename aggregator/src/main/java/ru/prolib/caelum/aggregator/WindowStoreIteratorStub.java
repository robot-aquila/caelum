package ru.prolib.caelum.aggregator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class WindowStoreIteratorStub<T> implements WindowStoreIterator<T> {
	private final LinkedList<KeyValue<Long,T>> list;
	private boolean closed;
	
	public WindowStoreIteratorStub(List<KeyValue<Long, T>> list) {
		this.list = new LinkedList<>(list);
	}
	
	public WindowStoreIteratorStub() {
		this(new ArrayList<>());
	}

	@Override
	public Long peekNextKey() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		if ( list.size() == 0 ) {
			throw new NoSuchElementException();
		}
		return list.get(0).key;
	}

	@Override
	public boolean hasNext() {
		return closed == false && list.size() > 0;
	}

	@Override
	public KeyValue<Long, T> next() {
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		if ( list.size() == 0 ) {
			throw new NoSuchElementException();
		}
		return list.removeFirst();
	}

	@Override
	public void close() {
		closed = true;
	}

}
