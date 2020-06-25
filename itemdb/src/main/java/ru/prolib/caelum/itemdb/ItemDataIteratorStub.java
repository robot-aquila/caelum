package ru.prolib.caelum.itemdb;

import java.util.NoSuchElementException;

import ru.prolib.caelum.core.IteratorStub;

public class ItemDataIteratorStub extends IteratorStub<IItemData> implements IItemDataIterator {

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public IItemData next() {
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
