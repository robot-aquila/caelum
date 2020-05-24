package ru.prolib.caelum.core;

import java.math.BigInteger;

import org.apache.kafka.streams.kstream.Aggregator;

public class LBTradeAggregator implements Aggregator<String, ILBTrade, LBCandleMutable> {

	@Override
	public LBCandleMutable apply(String symbol, ILBTrade trade, LBCandleMutable candle) {
		long trade_price = trade.getPrice(), trade_volume = trade.getVolume();
		byte trade_price_decimals = trade.getPriceDecimals(), trade_volume_decimals = trade.getVolumeDecimals();
		if ( candle.volume == null ) {
			candle.type = CandleRecordType.LONG_REGULAR;
			candle.open = candle.high = candle.low = candle.close = trade_price;
			candle.volume = trade_volume;
			candle.priceDecimals = trade_price_decimals;
			candle.volumeDecimals = trade_volume_decimals;
		} else {
			if ( candle.priceDecimals != trade_price_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Price decimals mismatch: symbol: ")
						.append(symbol)
						.append(", expected: ")
						.append(candle.priceDecimals)
						.append(", actual: ")
						.append(trade_price_decimals)
						.toString());
			}
			if ( candle.volumeDecimals != trade_volume_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Volume decimals mismatch: symbol: ")
						.append(symbol)
						.append(", expected: ")
						.append(candle.volumeDecimals)
						.append(", actual: ")
						.append(trade_volume_decimals)
						.toString());
			}
			if ( candle.bigVolume == null ) {
				try {
					candle.volume = Math.addExact(candle.volume, trade_volume);
				} catch ( ArithmeticException e ) {
					candle.bigVolume = BigInteger.valueOf(candle.volume).add(BigInteger.valueOf(trade_volume));
					candle.type = CandleRecordType.LONG_WIDEVOL;
					candle.volume = 0L;
				}
			} else {
				candle.bigVolume = candle.bigVolume.add(BigInteger.valueOf(trade_volume));
			}
			
			if ( trade_price > candle.high ) {
				candle.high = trade_price;
			}
			if ( trade_price < candle.low ) {
				candle.low = trade_price;
			}
			candle.close = trade_price;
		}
		return candle;
	}

}
