package ru.prolib.caelum.backnode.mvc;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.core.Tuple;

public class TupleMvcAdapterIterator implements WindowStoreIterator<Tuple> {
	private final WindowStoreIterator<Tuple> iterator;
	
	public TupleMvcAdapterIterator(WindowStoreIterator<Tuple> iterator) {
		this.iterator = iterator;
	}

	@Override
	public Long peekNextKey() {
		return iterator.peekNextKey();
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public TupleMvcAdapter next() {
		KeyValue<Long, Tuple> kv = iterator.next();
		return new TupleMvcAdapter(kv.key, kv.value);
	}

	@Override
	public void close() {
		iterator.close();
	}

}
