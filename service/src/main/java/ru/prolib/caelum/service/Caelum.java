package ru.prolib.caelum.service;

import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatorService;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;

public class Caelum implements ICaelum {
	private final AggregatorService aggrService;
	private final IItemDatabaseService itemDbService;
	
	public Caelum(AggregatorService aggregator_service, IItemDatabaseService itemdb_service) {
		this.aggrService = aggregator_service;
		this.itemDbService = itemdb_service;
	}

	@Override
	public WindowStoreIterator<Tuple> fetch(AggregatedDataRequest request) {
		return aggrService.fetch(request);
	}

	@Override
	public IItemDataIterator fetch(ItemDataRequest request) {
		return itemDbService.fetch(request);
	}

	@Override
	public IItemDataIterator fetch(ItemDataRequestContinue request) {
		return itemDbService.fetch(request);
	}

}
