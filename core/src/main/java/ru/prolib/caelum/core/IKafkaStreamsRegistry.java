package ru.prolib.caelum.core;

import org.apache.kafka.streams.KafkaStreams;

public interface IKafkaStreamsRegistry {

	/**
	 * Register one of OHLCV aggregation streams instance to query it's store.
	 * <p>
	 * @param period - period of aggregation.
	 * @param store_name - store name
	 * @param streams - streams where store is registered
	 */
	void registerOHLCVAggregator(Period period, String store_name, KafkaStreams streams);
		
}
