package ru.prolib.caelum.itemdb;

import java.time.Instant;

public interface IItemDataRequest {
	String getSymbol();
	long getTo();
	long getLimit();
	Instant getTimeTo();
}
