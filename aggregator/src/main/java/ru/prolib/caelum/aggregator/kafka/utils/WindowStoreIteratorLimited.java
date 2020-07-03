package ru.prolib.caelum.aggregator.kafka.utils;

import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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
	public int hashCode() {
		return new HashCodeBuilder(40991, 57219)
				.append(iterator)
				.append(limit)
				.append(passed)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != WindowStoreIteratorLimited.class ) {
			return false;
		}
		WindowStoreIteratorLimited<?> o = (WindowStoreIteratorLimited<?>) other;
		return new EqualsBuilder()
				.append(o.iterator, iterator)
				.append(o.limit, limit)
				.append(o.passed, passed)
				.build();
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
