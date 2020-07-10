package ru.prolib.caelum.itemdb;

import java.util.List;

import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.IteratorStub;

public class ItemIteratorStub extends IteratorStub<IItem> implements IItemIterator {
	protected final ItemDataResponse metaData;
	
	public ItemIteratorStub(List<IItem> data, ItemDataResponse meta_data, boolean make_copy) {
		super(data, make_copy);
		this.metaData = meta_data;
	}
	
	public ItemIteratorStub(ItemDataResponse meta_data) {
		super();
		this.metaData = meta_data;
	}
	
	public ItemIteratorStub() {
		this(null);
	}

	@Override
	public ItemDataResponse getMetaData() {
		if ( metaData == null ) {
			throw new IllegalStateException("No metadata");
		} else {
			return metaData;
		}
	}

}
