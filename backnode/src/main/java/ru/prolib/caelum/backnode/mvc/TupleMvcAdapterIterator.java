package ru.prolib.caelum.backnode.mvc;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;

public class TupleMvcAdapterIterator implements ICloseableIterator<TupleMvcAdapter> {
	private final ICloseableIterator<ITuple> iterator;
	
	public TupleMvcAdapterIterator(ICloseableIterator<ITuple> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public TupleMvcAdapter next() {
		return new TupleMvcAdapter(iterator.next());
	}

	@Override
	public void close() throws Exception {
		iterator.close();
	}

}
