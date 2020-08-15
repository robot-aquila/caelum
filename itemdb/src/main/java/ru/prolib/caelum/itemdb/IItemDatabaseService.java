package ru.prolib.caelum.itemdb;

import java.util.Collection;

import ru.prolib.caelum.core.IItem;

public interface IItemDatabaseService {
	void registerItem(IItem item);
	void registerItem(Collection<IItem> items);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	void clear(boolean global);
}
