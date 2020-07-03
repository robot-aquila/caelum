package ru.prolib.caelum.aggregator.kafka;

import org.apache.kafka.common.serialization.Serdes;

public class KafkaTupleSerde extends Serdes.WrapperSerde<KafkaTuple> {
	
	public KafkaTupleSerde() {
		super(new KafkaTupleSerializer(), new KafkaTupleDeserializer());
	}
	
	@Override
	public int hashCode() {
		return 7200172;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaTupleSerde.class ) {
			return false;
		}
		return true;
	}

}