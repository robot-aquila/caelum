package ru.prolib.caelum.service;

import java.time.Instant;

public interface IItemDataRequest {
	String getSymbol();
	Long getTo();
	Integer getLimit();
	Instant getTimeTo();
}
