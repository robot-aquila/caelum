package ru.prolib.caelum.service;

import java.util.Collection;
import java.util.List;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.aggregator.IAggregatorService;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.symboldb.ISymbolService;

public class Caelum implements ICaelum {
	private final IAggregatorService aggrService;
	private final IItemDatabaseService itemDbService;
	private final ISymbolService symbolService;
	private final Collection<IExtension> extensions;
	
	public Caelum(IAggregatorService aggregator_service,
			IItemDatabaseService itemdb_service,
			ISymbolService symbol_service,
			Collection<IExtension> extensions)
	{
		this.aggrService = aggregator_service;
		this.itemDbService = itemdb_service;
		this.symbolService = symbol_service;
		this.extensions = extensions;
	}
	
	public IAggregatorService getAggregatorService() {
		return aggrService;
	}
	
	public IItemDatabaseService getItemDatabaseService() {
		return itemDbService;
	}
	
	public ISymbolService getSymbolService() {
		return symbolService;
	}
	
	public Collection<IExtension> getExtensions() {
		return extensions;
	}
	
	@Override
	public void registerSymbol(String symbol) {
		symbolService.registerSymbol(symbol);
	}
	
	@Override
	public void registerSymbol(Collection<String> symbols) {
		symbolService.registerSymbol(symbols);
	}

	@Override
	public void registerEvents(Events events) {
		symbolService.registerEvents(events);
	}
	
	@Override
	public void registerEvents(Collection<Events> events) {
		symbolService.registerEvents(events);
	}
	
	@Override
	public void deleteEvents(Events events) {
		symbolService.deleteEvents(events);
	}
	
	@Override
	public void deleteEvents(Collection<Events> events) {
		symbolService.deleteEvents(events);
	}

	@Override
	public AggregatedDataResponse fetch(AggregatedDataRequest request) {
		return aggrService.fetch(request);
	}

	@Override
	public IItemIterator fetch(ItemDataRequest request) {
		return itemDbService.fetch(request);
	}

	@Override
	public IItemIterator fetch(ItemDataRequestContinue request) {
		return itemDbService.fetch(request);
	}
	
	@Override
	public void registerItem(IItem item) {
		itemDbService.registerItem(item);
	}
	
	@Override
	public void registerItem(Collection<IItem> items) {
		itemDbService.registerItem(items);
	}

	@Override
	public ICloseableIterator<String> fetchCategories() {
		return symbolService.listCategories();
	}

	@Override
	public ICloseableIterator<String> fetchSymbols(SymbolListRequest request) {
		return symbolService.listSymbols(request);
	}

	@Override
	public ICloseableIterator<Events> fetchEvents(EventListRequest request) {
		return symbolService.listEvents(request);
	}
	
	@Override
	public void clear(boolean global) {
		symbolService.clear(global);
		itemDbService.clear(global);
		aggrService.clear(global);
		for ( IExtension extension : extensions ) {
			extension.clear();
		}
	}

	@Override
	public List<Interval> getAggregationIntervals() {
		return aggrService.getAggregationIntervals();
	}

	@Override
	public List<AggregatorStatus> getAggregatorStatus() {
		return aggrService.getAggregatorStatus();
	}

}
