package ru.prolib.caelum.service;

import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;

/**
 * Caelum facade interface.
 */
public interface ICaelum {
	WindowStoreIterator<Tuple> fetch(AggregatedDataRequest request);
	IItemDataIterator fetch(ItemDataRequest request);
	IItemDataIterator fetch(ItemDataRequestContinue request);
}
