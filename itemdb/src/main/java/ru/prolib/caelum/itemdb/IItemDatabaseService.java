package ru.prolib.caelum.itemdb;

public interface IItemDatabaseService {
	IItemDataIterator fetch(ItemDataRequest request);
	IItemDataIterator fetch(ItemDataRequestContinue request);
}
