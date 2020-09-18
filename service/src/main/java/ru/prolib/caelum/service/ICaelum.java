package ru.prolib.caelum.service;

import java.util.Collection;
import java.util.List;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatedDataResponse;
import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.Interval;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.symboldb.EventListRequest;
import ru.prolib.caelum.symboldb.SymbolListRequest;

/**
 * Caelum facade interface.
 */
public interface ICaelum {
	void registerSymbol(String symbol);
	void registerSymbol(Collection<String> symbols);
	void registerEvents(Events events);
	void registerEvents(Collection<Events> events);
	void registerItem(IItem item);
	void registerItem(Collection<IItem> items);
	void deleteEvents(Events events);
	void deleteEvents(Collection<Events> events);
	AggregatedDataResponse fetch(AggregatedDataRequest request);
	IItemIterator fetch(ItemDataRequest request);
	IItemIterator fetch(ItemDataRequestContinue request);
	ICloseableIterator<String> fetchCategories();
	ICloseableIterator<String> fetchSymbols(SymbolListRequest request);
	ICloseableIterator<Events> fetchEvents(EventListRequest request);
	void clear(boolean global);
	List<Interval> getAggregationIntervals();
	List<AggregatorStatus> getAggregatorStatus();
}
