package ru.prolib.caelum.aggregator.kafka;

import java.time.Duration;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.state.WindowStoreIterator;

public class KafkaTupleAggregateIterator implements WindowStoreIterator<KafkaTuple> {
	private final WindowStoreIterator<KafkaTuple> source;
	private final Duration intervalDuration;
	private final Aggregator<String, KafkaTuple, KafkaTuple> aggregator;
	private KeyValue<Long, KafkaTuple> next, last;
	private boolean hasItemUnderCursor = false, closed = false;
	
	public KafkaTupleAggregateIterator(WindowStoreIterator<KafkaTuple> source,
			Duration newIntervalDuration,
			Aggregator<String, KafkaTuple, KafkaTuple> aggregator)
	{
		this.source = source;
		this.intervalDuration = newIntervalDuration;
		this.aggregator = aggregator;
		advance();
	}
	
	public KafkaTupleAggregateIterator(WindowStoreIterator<KafkaTuple> source, Duration newIntervalDuration) {
		this(source, newIntervalDuration, new KafkaTupleAggregator());
	}
	
	private void advance() {
		next = null;
		if ( ! hasItemUnderCursor ) {
			if ( (hasItemUnderCursor = source.hasNext()) == false ) {
				return;
			}
			last = source.next();
		}

		long interval_millis = intervalDuration.toMillis();
		long time_round = last.key / interval_millis;
		KafkaTuple aggregate = aggregator.apply(null, last.value, new KafkaTuple());
		while ( (hasItemUnderCursor = source.hasNext()) == true ) {
			last = source.next();
			if ( last.key / interval_millis != time_round ) {
				break;
			}
			aggregate = aggregator.apply(null, last.value, aggregate);
		}
		next = new KeyValue<>(time_round * interval_millis, aggregate);
	}
	
	@Override
	public Long peekNextKey() {
		if ( next == null ) {
			throw new NoSuchElementException();
		}
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		return next.key;
	}

	@Override
	public boolean hasNext() {
		return closed == false && next != null;
	}

	@Override
	public KeyValue<Long, KafkaTuple> next() {
		if ( next == null ) {
			throw new NoSuchElementException();
		}
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		KeyValue<Long, KafkaTuple> r = next;
		advance();
		return r;
	}

	@Override
	public void close() {
		if ( ! closed ) {
			closed = true;
			source.close();
		}
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(502227, 703)
				.append(source)
				.append(intervalDuration)
				.append(aggregator)
				.append(closed)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaTupleAggregateIterator.class ) {
			return false;
		}
		KafkaTupleAggregateIterator o = (KafkaTupleAggregateIterator) other;
		return new EqualsBuilder()
				.append(o.source, source)
				.append(o.intervalDuration, intervalDuration)
				.append(o.aggregator, aggregator)
				.append(o.closed, closed)
				.append(o.next, next)
				.append(o.last, last)
				.append(o.hasItemUnderCursor, hasItemUnderCursor)
				.build();
	}
	
}
