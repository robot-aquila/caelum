package ru.prolib.caelum.service.itemdb;

import java.util.Collection;

import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.ItemDataRequest;
import ru.prolib.caelum.service.ItemDataRequestContinue;

public interface IItemDatabaseService {
	void registerItem(IItem item);
	void registerItem(Collection<IItem> items);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	void clear(boolean global);
}
