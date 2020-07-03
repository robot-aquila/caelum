package ru.prolib.caelum.service;

import ru.prolib.caelum.aggregator.kafka.AggregatorService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.symboldb.ISymbolService;

public class CaelumBuilder {
	private AggregatorService aggrService;
	private IItemDatabaseService itemDbService;
	private ISymbolService symbolService;
	
	public CaelumBuilder withAggregatorService(AggregatorService service) {
		this.aggrService = service;
		return this;
	}
	
	public CaelumBuilder withItemDatabaseService(IItemDatabaseService service) {
		this.itemDbService = service;
		return this;
	}
	
	public CaelumBuilder withSymbolService(ISymbolService service) {
		this.symbolService = service;
		return this;
	}
	
	public ICaelum build() {
		if ( aggrService == null ) {
			throw new NullPointerException("Aggregator service was not defined");
		}
		if ( itemDbService == null ) {
			throw new NullPointerException("ItemDB service was not defined");
		}
		if ( symbolService == null ) {
			throw new NullPointerException("Symbol service was not defined");
		}
		return new Caelum(aggrService, itemDbService, symbolService);
	}
	
}
