package ru.prolib.caelum.aggregator.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class KafkaTupleSerdes {
	
	public static Serde<KafkaTuple> tupleSerde() {
		return new KafkaTupleSerde();
	}
	
	public static Serde<String> keySerde() {
		return Serdes.String();
	}

}
