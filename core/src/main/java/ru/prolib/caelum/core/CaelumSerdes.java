package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CaelumSerdes {
	
	public static class ILBTradeSerde extends Serdes.WrapperSerde<ILBTrade> {
		public ILBTradeSerde() {
			super(new LBTradeSerializer(), new LBTradeDeserializer());
		}
	}
	
	public static class ILBCandleSerde extends Serdes.WrapperSerde<ILBCandle> {
		public ILBCandleSerde() {
			super(new LBCandleSerializer<>(), new LBCandleDeserializer());
		}
	}
	
	public static class LBCandleMutableSerde extends Serdes.WrapperSerde<LBCandleMutable> {
		public LBCandleMutableSerde() {
			super(new LBCandleSerializer<>(), new LBCandleMutableDeserializer());
		}
	}

	public static Serde<ILBTrade> ILBTrade() {
		return new ILBTradeSerde();
	}
	
	public static Serde<ILBCandle> ILBCandle() {
		return new ILBCandleSerde();
	}
	
	public static Serde<LBCandleMutable> LBCandleMutable() {
		return new LBCandleMutableSerde();
	}

}
