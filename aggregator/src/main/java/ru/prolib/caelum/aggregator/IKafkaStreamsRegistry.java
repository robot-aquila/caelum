package ru.prolib.caelum.aggregator;

import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.core.Period;

@Deprecated
public interface IKafkaStreamsRegistry {

	/**
	 * Register one of data aggregation streams instance to query it's store.
	 * <p>
	 * @param period - period of aggregation.
	 * @param store_name - store name
	 * @param streams - streams where store is registered
	 */
	void registerAggregator(Period period, String store_name, KafkaStreams streams);
		
}
