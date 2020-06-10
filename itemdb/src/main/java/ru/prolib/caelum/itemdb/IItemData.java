package ru.prolib.caelum.itemdb;

import ru.prolib.caelum.core.Item;

public interface IItemData {
	String getSymbol();
	long getTime();
	Item getItem();
}
