package ru.prolib.caelum.itemdb.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class KafkaItemSerdes {
	
	public static Serde<KafkaItem> itemSerde() {
		return new KafkaItemSerde();
	}
	
	public static Serde<String> keySerde() {
		return Serdes.String();
	}

}
