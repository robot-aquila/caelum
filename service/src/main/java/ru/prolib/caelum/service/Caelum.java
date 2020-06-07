package ru.prolib.caelum.service;

import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatorService;
import ru.prolib.caelum.core.Tuple;

public class Caelum implements ICaelum {
	private final AggregatorService aggregatorService;
	
	public Caelum(AggregatorService aggregator_service) {
		this.aggregatorService = aggregator_service;
	}

	@Override
	public WindowStoreIterator<Tuple> fetch(AggregatedDataRequest request) {
		return aggregatorService.fetch(request);
	}

}
