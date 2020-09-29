package ru.prolib.caelum.service;

import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.IItem;

public interface IItemIterator extends ICloseableIterator<IItem> {
	ItemDataResponse getMetaData();
}
