package ru.prolib.caelum.itemdb;

import java.time.Instant;

import ru.prolib.caelum.core.Item;

public interface IItemData {
	String getSymbol();
	long getTime();
	long getOffset();
	Item getItem();
	Instant getTimeAsInstant();
}
