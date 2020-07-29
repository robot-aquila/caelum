package ru.prolib.caelum.itemdb;

import ru.prolib.caelum.core.IItem;

public interface IItemDatabaseService {
	void registerItem(IItem item);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	void clear(boolean global);
}
