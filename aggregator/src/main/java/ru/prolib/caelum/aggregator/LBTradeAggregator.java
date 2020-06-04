package ru.prolib.caelum.aggregator;

import static ru.prolib.caelum.core.OHLCVRecordType.*;

import java.math.BigInteger;

import org.apache.kafka.streams.kstream.Aggregator;

import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBOHLCVMutable;

public class LBTradeAggregator implements Aggregator<String, ILBTrade, LBOHLCVMutable> {

	@Override
	public LBOHLCVMutable apply(String key, ILBTrade value, LBOHLCVMutable aggregate) {
		long trade_price = value.getPrice(), trade_volume = value.getVolume();
		byte trade_price_decimals = value.getPriceDecimals(), trade_volume_decimals = value.getVolumeDecimals();
		if ( aggregate.volume == null ) {
			aggregate.type = LONG_REGULAR;
			aggregate.open = aggregate.high = aggregate.low = aggregate.close = trade_price;
			aggregate.volume = trade_volume;
			aggregate.priceDecimals = trade_price_decimals;
			aggregate.volumeDecimals = trade_volume_decimals;
		} else {
			if ( aggregate.priceDecimals != trade_price_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Price decimals mismatch: symbol: ")
						.append(key)
						.append(", expected: ")
						.append(aggregate.priceDecimals)
						.append(", actual: ")
						.append(trade_price_decimals)
						.toString());
			}
			if ( aggregate.volumeDecimals != trade_volume_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Volume decimals mismatch: symbol: ")
						.append(key)
						.append(", expected: ")
						.append(aggregate.volumeDecimals)
						.append(", actual: ")
						.append(trade_volume_decimals)
						.toString());
			}
			if ( aggregate.bigVolume == null ) {
				try {
					aggregate.volume = Math.addExact(aggregate.volume, trade_volume);
				} catch ( ArithmeticException e ) {
					aggregate.bigVolume = BigInteger.valueOf(aggregate.volume).add(BigInteger.valueOf(trade_volume));
					aggregate.type = LONG_WIDEVOL;
					aggregate.volume = 0L;
				}
			} else {
				aggregate.bigVolume = aggregate.bigVolume.add(BigInteger.valueOf(trade_volume));
			}
			
			if ( trade_price > aggregate.high ) {
				aggregate.high = trade_price;
			}
			if ( trade_price < aggregate.low ) {
				aggregate.low = trade_price;
			}
			aggregate.close = trade_price;
		}
		return aggregate;
	}

}
