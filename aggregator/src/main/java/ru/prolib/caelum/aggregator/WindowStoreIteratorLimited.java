package ru.prolib.caelum.aggregator;

import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class WindowStoreIteratorLimited<T> implements WindowStoreIterator<T> {
	private final WindowStoreIterator<T> iterator;
	private final long limit;
	private long passed;
	
	public WindowStoreIteratorLimited(WindowStoreIterator<T> iterator, long limit) {
		this.iterator = iterator;
		this.limit = limit;
	}

	@Override
	public Long peekNextKey() {
		if ( passed >= limit ) {
			throw new NoSuchElementException();
		}
		return iterator.peekNextKey();
	}

	@Override
	public boolean hasNext() {
		if ( passed >= limit ) {
			return false;
		}
		return iterator.hasNext();
	}

	@Override
	public KeyValue<Long, T> next() {
		if ( passed >= limit ) {
			throw new NoSuchElementException();
		}
		passed ++;
		return iterator.next();
	}

	@Override
	public void close() {
		iterator.close();
	}

}
