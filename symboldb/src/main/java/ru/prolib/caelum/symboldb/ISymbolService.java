package ru.prolib.caelum.symboldb;

import ru.prolib.caelum.core.ICloseableIterator;

public interface ISymbolService {
	void registerSymbol(String symbol);
	void registerSymbolUpdate(SymbolUpdate update);
	ICloseableIterator<String> listCategories();
	ICloseableIterator<String> listSymbols(SymbolListRequest request);
	ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol);
	void clear();
}
