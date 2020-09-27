package ru.prolib.caelum.backnode.mvc;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import ru.prolib.caelum.service.itemdb.IItemIterator;
import ru.prolib.caelum.service.itemdb.ItemDataResponse;

public class ItemMvcAdapterIterator implements Iterator<ItemMvcAdapter>, Closeable {
	private final IItemIterator iterator;
	private ItemDataResponse metaData;
	
	public ItemMvcAdapterIterator(IItemIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public ItemMvcAdapter next() {
		return new ItemMvcAdapter(iterator.next());
	}

	@Override
	public void close() throws IOException {
		try {
			iterator.close();
		} catch ( Exception e ) {
			throw new IOException(e);
		}
	}
	
	public ItemDataResponse getMetaData() {
		if ( metaData == null ) {
			metaData = iterator.getMetaData();
		}
		return metaData;
	}
	
}
