package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class LBCandleSerde implements Serde<ILBCandle> {
	private final Deserializer<ILBCandle> deserializer = new LBCandleDeserializer();
	private final Serializer<ILBCandle> serializer = new LBCandleSerializer();

	@Override
	public Deserializer<ILBCandle> deserializer() {
		return deserializer;
	}

	@Override
	public Serializer<ILBCandle> serializer() {
		return serializer;
	}

}
