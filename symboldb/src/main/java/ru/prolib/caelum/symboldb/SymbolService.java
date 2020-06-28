package ru.prolib.caelum.symboldb;

import ru.prolib.caelum.core.ICloseableIterator;

public interface SymbolService {
	void registerSymbol(String symbol);
	void registerSymbolUpdate(SymbolUpdate update);
	ICloseableIterator<String> listCategories();
	ICloseableIterator<String> listSymbols(SymbolListRequest request);
	ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol);
}
