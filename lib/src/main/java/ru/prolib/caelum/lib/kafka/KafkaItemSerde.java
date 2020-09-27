package ru.prolib.caelum.lib.kafka;

import org.apache.kafka.common.serialization.Serdes;

public class KafkaItemSerde extends Serdes.WrapperSerde<KafkaItem> {
	
	public KafkaItemSerde() {
		super(new KafkaItemSerializer(), new KafkaItemDeserializer());
	}
	
	@Override
	public int hashCode() {
		return 30719;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemSerde.class ) {
			return false;
		}
		return true;
	}

}