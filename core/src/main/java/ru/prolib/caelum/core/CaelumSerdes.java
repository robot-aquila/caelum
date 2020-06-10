package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CaelumSerdes {
	
	public static class ItemSerde extends Serdes.WrapperSerde<Item> {
		public ItemSerde() {
			super(new ItemSerializer(), new ItemDeserializer());
		}
	}
	
	public static class TupleSerde extends Serdes.WrapperSerde<Tuple> {
		public TupleSerde() {
			super(new TupleSerializer(), new TupleDeserializer());
		}
	}

	public static Serde<Item> itemSerde() {
		return new ItemSerde();
	}
	
	public static Serde<Tuple> tupleSerde() {
		return new TupleSerde();
	}
	
	public static Serde<String> keySerde() {
		return Serdes.String();
	}

}
