package ru.prolib.caelum.service;

import ru.prolib.caelum.aggregator.AggregatorService;

public class CaelumBuilder {
	private AggregatorService aggregatorService;
	
	public CaelumBuilder withAggregatorService(AggregatorService service) {
		this.aggregatorService = service;
		return this;
	}
	
	public ICaelum build() {
		if ( aggregatorService == null ) {
			throw new NullPointerException("Aggregator service was not defined");
		}
		return new Caelum(aggregatorService);
	}
	
}
