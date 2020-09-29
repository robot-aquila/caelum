package ru.prolib.caelum.service.itemdb;

import java.util.List;

import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.lib.IteratorStub;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.ItemDataResponse;

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
