package ru.prolib.caelum.core;

import java.math.BigInteger;

public class LBCandleMutableDeserializer extends AbstractILBCandleDeserializer<LBCandleMutable> {

	@Override
	protected LBCandleMutable produce(long open, long high, long low, long close, byte price_decimals,
			long volume, BigInteger big_volume, byte volume_decimals, CandleRecordType type)
	{
		return new LBCandleMutable(open, high, low, close, price_decimals, volume, big_volume, volume_decimals, type);
	}

}
