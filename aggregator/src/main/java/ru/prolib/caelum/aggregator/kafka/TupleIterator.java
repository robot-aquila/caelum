package ru.prolib.caelum.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;

public class TupleIterator implements ICloseableIterator<ITuple> {
	private final String symbol;
	private final WindowStoreIterator<KafkaTuple> iterator;
	
	public TupleIterator(String symbol, WindowStoreIterator<KafkaTuple> iterator) {
		this.symbol = symbol;
		this.iterator = iterator;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public WindowStoreIterator<KafkaTuple> getSource() {
		return iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public ITuple next() {
		KeyValue<Long, KafkaTuple> kv = iterator.next();
		return new Tuple(symbol, kv.key, kv.value);
	}

	@Override
	public void close() throws Exception {
		iterator.close();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(901878297, 3091)
				.append(symbol)
				.append(iterator)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != TupleIterator.class ) {
			return false;
		}
		TupleIterator o = (TupleIterator) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.iterator, iterator)
				.build();
	}

}
