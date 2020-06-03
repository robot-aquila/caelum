package ru.prolib.caelum.core;

import java.math.BigInteger;

public class LBOHLCVDeserializer extends AbstractILBOHLCVDeserializer<ILBOHLCV> {

	@Override
	protected ILBOHLCV produce(long open, long high, long low, long close, byte price_decimals,
			long volume, BigInteger big_volume, byte volume_decimals, OHLCVRecordType type)
	{
		return new LBOHLCV(open, high, low, close, price_decimals, volume, big_volume, volume_decimals, type);
	}
	

}
