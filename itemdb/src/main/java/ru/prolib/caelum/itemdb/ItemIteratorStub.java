package ru.prolib.caelum.itemdb;

import java.util.NoSuchElementException;

import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.IteratorStub;

public class ItemIteratorStub extends IteratorStub<IItem> implements IItemIterator {

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public IItem next() {
		throw new NoSuchElementException();
	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public ItemDataResponse getMetaData() {
		throw new IllegalStateException("No metadata");
	}

}
