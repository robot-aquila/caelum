package ru.prolib.caelum.service;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
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
	
	public Caelum(IAggregatorService aggregator_service,
			IItemDatabaseService itemdb_service,
			ISymbolService symbol_service)
	{
		this.aggrService = aggregator_service;
		this.itemDbService = itemdb_service;
		this.symbolService = symbol_service;
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
	
	@Override
	public void registerSymbol(String symbol) {
		symbolService.registerSymbol(symbol);
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

}
