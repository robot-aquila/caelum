package ru.prolib.caelum.itemdb;

public interface IItemDatabaseService {
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
}
