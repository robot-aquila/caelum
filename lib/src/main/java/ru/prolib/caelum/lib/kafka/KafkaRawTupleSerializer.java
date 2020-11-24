package ru.prolib.caelum.lib.kafka;

import org.apache.kafka.common.serialization.Serializer;

import ru.prolib.caelum.lib.data.RawTuple;
import ru.prolib.caelum.lib.data.pk1.Pk1Utils;

public class KafkaRawTupleSerializer implements Serializer<RawTuple> {
	private final Pk1Utils utils;
	
	public KafkaRawTupleSerializer(Pk1Utils utils) {
		this.utils = utils;
	}

	@Override
	public byte[] serialize(String topic, RawTuple tuple) {
	    var pk = utils.toTuplePk(tuple);
		
		// TODO Auto-generated method stub
		return null;
	}

}
