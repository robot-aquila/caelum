package ru.prolib.caelum.aggregator;

import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.core.Period;

public class KafkaStreamsRegistryStub implements IKafkaStreamsRegistry {

	@Override
	public void registerAggregator(Period period, String store_name, KafkaStreams streams) {
		
	}

}
