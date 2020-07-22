package ru.prolib.caelum.service;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

/**
 * Caelum facade interface.
 */
public interface ICaelum {
	void registerSymbol(String symbol);
	void registerSymbolUpdate(SymbolUpdate update);
	void registerItem(IItem item);
	ICloseableIterator<ITuple> fetch(AggregatedDataRequest request);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	ICloseableIterator<String> fetchCategories();
	ICloseableIterator<String> fetchSymbols(SymbolListRequest request);
	ICloseableIterator<SymbolUpdate> fetchSymbolUpdates(String symbol);
	void clear();
}
