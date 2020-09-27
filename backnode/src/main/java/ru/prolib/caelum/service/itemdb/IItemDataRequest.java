package ru.prolib.caelum.service.itemdb;

import java.time.Instant;

public interface IItemDataRequest {
	String getSymbol();
	Long getTo();
	Integer getLimit();
	Instant getTimeTo();
}
