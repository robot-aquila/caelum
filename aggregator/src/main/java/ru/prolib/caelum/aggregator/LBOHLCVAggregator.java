package ru.prolib.caelum.aggregator;

import static ru.prolib.caelum.core.OHLCVRecordType.*;

import java.math.BigInteger;

import org.apache.kafka.streams.kstream.Aggregator;

import ru.prolib.caelum.core.LBOHLCVMutable;

public class LBOHLCVAggregator implements Aggregator<String, LBOHLCVMutable, LBOHLCVMutable> {

	@Override
	public LBOHLCVMutable apply(String key, LBOHLCVMutable value, LBOHLCVMutable aggregate) {
		if ( aggregate.volume == null ) {
			aggregate.type = value.type;
			aggregate.open = value.open;
			aggregate.high = value.high;
			aggregate.low = value.low;
			aggregate.close = value.close;
			aggregate.volume = value.volume;
			aggregate.bigVolume = value.bigVolume;
			aggregate.priceDecimals = value.priceDecimals;
			aggregate.volumeDecimals = value.volumeDecimals;
		} else {
			if ( aggregate.priceDecimals != value.priceDecimals ) {
				throw new IllegalArgumentException(new StringBuilder()
					.append("Price decimals mismatch: symbol: ")
					.append(key)
					.append(", expected: ")
					.append(aggregate.priceDecimals)
					.append(", actual: ")
					.append(value.priceDecimals)
					.toString());
			}
			if ( aggregate.volumeDecimals != value.volumeDecimals ) {
				throw new IllegalArgumentException(new StringBuilder()
					.append("Volume decimals mismatch: symbol: ")
					.append(key)
					.append(", expected: ")
					.append(aggregate.volumeDecimals)
					.append(", actual: ")
					.append(value.volumeDecimals)
					.toString());
			}
			
			if ( aggregate.bigVolume == null && value.bigVolume == null ) {
				try {
					aggregate.volume = Math.addExact(aggregate.volume, value.volume);
				} catch ( ArithmeticException e ) {
					aggregate.bigVolume = BigInteger.valueOf(aggregate.volume).add(BigInteger.valueOf(value.volume));
					aggregate.volume = 0L;
					aggregate.type = LONG_WIDEVOL;
				}
			} else if ( aggregate.bigVolume == null ) {
				aggregate.bigVolume = value.bigVolume.add(BigInteger.valueOf(aggregate.volume));
				aggregate.volume = 0L;
				aggregate.type = LONG_WIDEVOL;				
			} else if ( value.bigVolume == null ) {
				aggregate.bigVolume = aggregate.bigVolume.add(BigInteger.valueOf(value.volume));
			} else {
				aggregate.bigVolume = aggregate.bigVolume.add(value.bigVolume);
			}
			
			if ( value.high > aggregate.high ) {
				aggregate.high = value.high;
			}
			if ( value.low < aggregate.low ) {
				aggregate.low = value.low;
			}
			aggregate.close = value.close;
		}
		return aggregate;
	}

}
