package ru.prolib.caelum.itemdb;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.IItem;

public interface IItemIterator extends ICloseableIterator<IItem> {
	ItemDataResponse getMetaData();
}
