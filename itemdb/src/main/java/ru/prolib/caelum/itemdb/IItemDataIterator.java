package ru.prolib.caelum.itemdb;

import java.util.Iterator;

public interface IItemDataIterator extends Iterator<IItemData> {
	ItemDataResponseMeta close();
}
