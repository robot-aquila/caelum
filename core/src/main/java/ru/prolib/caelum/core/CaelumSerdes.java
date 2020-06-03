package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CaelumSerdes {
	
	public static class ILBTradeSerde extends Serdes.WrapperSerde<ILBTrade> {
		public ILBTradeSerde() {
			super(new LBTradeSerializer(), new LBTradeDeserializer());
		}
	}
	
	public static class ILBOHLCVSerde extends Serdes.WrapperSerde<ILBOHLCV> {
		public ILBOHLCVSerde() {
			super(new LBOHLCVSerializer<>(), new LBOHLCVDeserializer());
		}
	}
	
	public static class LBOHLCVMutableSerde extends Serdes.WrapperSerde<LBOHLCVMutable> {
		public LBOHLCVMutableSerde() {
			super(new LBOHLCVSerializer<>(), new LBOHLCVMutableDeserializer());
		}
	}

	public static Serde<ILBTrade> ILBTrade() {
		return new ILBTradeSerde();
	}
	
	public static Serde<ILBOHLCV> ILBOHLCV() {
		return new ILBOHLCVSerde();
	}
	
	public static Serde<LBOHLCVMutable> LBOHLCVMutable() {
		return new LBOHLCVMutableSerde();
	}

}
