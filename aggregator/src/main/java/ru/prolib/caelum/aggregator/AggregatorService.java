package ru.prolib.caelum.aggregator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.core.Period;

public class AggregatorService {
	
	static class Entry {
		final AggregatorDesc desc;
		final KafkaStreams streams;
		
		Entry(AggregatorDesc desc, KafkaStreams streams) {
			this.desc = desc;
			this.streams = streams;
		}
	}
	
	private final Map<Period, Entry> entryByPeriod = new ConcurrentHashMap<>();
	private final Map<String, Entry> entryByStoreName = new ConcurrentHashMap<>();
	
	public void register(AggregatorDesc desc, KafkaStreams streams) {
		
	}

}
