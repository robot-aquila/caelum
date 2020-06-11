package ru.prolib.caelum.itemdb;

import ru.prolib.caelum.core.ICloseableIterator;

public interface IItemDataIterator extends ICloseableIterator<IItemData> {
	ItemDataResponse getMetaData();
}
