package ru.prolib.caelum.service;

public interface ISymbolCache {
	void registerSymbol(String symbol);
	void setRegistered(String symbol);
	void clear();
}
