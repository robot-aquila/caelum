package ru.prolib.caelum.service;

import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.Tuple;

/**
 * Caelum facade interface.
 */
public interface ICaelum {
	WindowStoreIterator<Tuple> fetch(AggregatedDataRequest request);
}
