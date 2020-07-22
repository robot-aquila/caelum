package ru.prolib.caelum.itemdb;

import java.time.Instant;

public interface IItemDataRequest {
	String getSymbol();
	Long getTo();
	Integer getLimit();
	Instant getTimeTo();
}
