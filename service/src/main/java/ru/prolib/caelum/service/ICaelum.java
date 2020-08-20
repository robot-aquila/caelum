package ru.prolib.caelum.service;

import java.util.Collection;
import java.util.List;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatedDataResponse;
import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.Period;
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
	void registerSymbol(Collection<String> symbols);
	void registerSymbolUpdate(SymbolUpdate update);
	void registerItem(IItem item);
	void registerItem(Collection<IItem> items);
	AggregatedDataResponse fetch(AggregatedDataRequest request);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	ICloseableIterator<String> fetchCategories();
	ICloseableIterator<String> fetchSymbols(SymbolListRequest request);
	ICloseableIterator<SymbolUpdate> fetchSymbolUpdates(String symbol);
	void clear(boolean global);
	List<Period> getAggregationPeriods();
	List<AggregatorStatus> getAggregatorStatus();
}
