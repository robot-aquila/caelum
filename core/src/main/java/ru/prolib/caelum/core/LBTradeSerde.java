package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class LBTradeSerde implements Serde<ILBTrade> {
	private final Deserializer<ILBTrade> deserializer = new LBTradeDeserializer();
	private final Serializer<ILBTrade> serializer = new LBTradeSerializer();

	@Override
	public Deserializer<ILBTrade> deserializer() {
		return deserializer;
	}

	@Override
	public Serializer<ILBTrade> serializer() {
		return serializer;
	}

}
