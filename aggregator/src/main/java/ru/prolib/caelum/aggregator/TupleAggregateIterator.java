package ru.prolib.caelum.aggregator;

import java.time.Duration;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.core.Tuple;

public class TupleAggregateIterator implements WindowStoreIterator<Tuple> {
	private final WindowStoreIterator<Tuple> source;
	private final Duration period;
	private final Aggregator<String, Tuple, Tuple> aggregator;
	private KeyValue<Long, Tuple> next, last;
	private boolean hasItemUnderCursor = false, closed = false;
	
	public TupleAggregateIterator(WindowStoreIterator<Tuple> source,
			Duration new_period,
			Aggregator<String, Tuple, Tuple> aggregator)
	{
		this.source = source;
		this.period = new_period;
		this.aggregator = aggregator;
		advance();
	}
	
	public TupleAggregateIterator(WindowStoreIterator<Tuple> source, Duration new_period) {
		this(source, new_period, new TupleAggregator());
	}
	
	private void advance() {
		next = null;
		if ( ! hasItemUnderCursor ) {
			if ( (hasItemUnderCursor = source.hasNext()) == false ) {
				return;
			}
			last = source.next();
		}

		long period_millis = period.toMillis();
		long time_round = last.key / period_millis;
		Tuple aggregate = aggregator.apply(null, last.value, new Tuple());
		while ( (hasItemUnderCursor = source.hasNext()) == true ) {
			last = source.next();
			if ( last.key / period_millis != time_round ) {
				break;
			}
			aggregate = aggregator.apply(null, last.value, aggregate);
		}
		next = new KeyValue<>(time_round * period_millis, aggregate);
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
	public KeyValue<Long, Tuple> next() {
		if ( next == null ) {
			throw new NoSuchElementException();
		}
		if ( closed ) {
			throw new IllegalStateException("Iterator already closed");
		}
		KeyValue<Long, Tuple> r = next;
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

}
