package ru.prolib.caelum.core;

import org.apache.kafka.streams.KafkaStreams;

public class KafkaStreamsRegistryStub implements IKafkaStreamsRegistry {

	@Override
	public void registerOHLCVAggregator(String period, String store_name, KafkaStreams streams) {
		
	}

}
