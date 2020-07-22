package ru.prolib.caelum.symboldb;

import java.util.Collection;

import ru.prolib.caelum.core.ICloseableIterator;

public interface ISymbolService {
	void registerSymbol(String symbol);
	void registerSymbol(Collection<String> symbols);
	void registerSymbolUpdate(SymbolUpdate update);
	ICloseableIterator<String> listCategories();
	ICloseableIterator<String> listSymbols(SymbolListRequest request);
	ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol);
	void clear();
}
