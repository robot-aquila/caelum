package ru.prolib.caelum.service;

import ru.prolib.caelum.aggregator.AggregatorService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;

public class CaelumBuilder {
	private AggregatorService aggrService;
	private IItemDatabaseService itemDbService;
	
	public CaelumBuilder withAggregatorService(AggregatorService service) {
		this.aggrService = service;
		return this;
	}
	
	public CaelumBuilder withItemDatabaseService(IItemDatabaseService service) {
		this.itemDbService = service;
		return this;
	}
	
	public ICaelum build() {
		if ( aggrService == null ) {
			throw new NullPointerException("Aggregator service was not defined");
		}
		if ( itemDbService == null ) {
			throw new NullPointerException("ItemDB service was not defined");
		}
		return new Caelum(aggrService, itemDbService);
	}
	
}
