package ru.prolib.caelum.service;

import java.util.Collection;
import java.util.List;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

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
	public void registerSymbolUpdate(SymbolUpdate update) {
		symbolService.registerSymbolUpdate(update);
	}

	@Override
	public ICloseableIterator<ITuple> fetch(AggregatedDataRequest request) {
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
	public ICloseableIterator<String> fetchCategories() {
		return symbolService.listCategories();
	}

	@Override
	public ICloseableIterator<String> fetchSymbols(SymbolListRequest request) {
		return symbolService.listSymbols(request);
	}

	@Override
	public ICloseableIterator<SymbolUpdate> fetchSymbolUpdates(String symbol) {
		return symbolService.listSymbolUpdates(symbol);
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
	public List<Period> getAggregationPeriods() {
		return aggrService.getAggregationPeriods();
	}

	@Override
	public List<AggregatorStatus> getAggregatorStatus() {
		return aggrService.getAggregatorStatus();
	}

}
