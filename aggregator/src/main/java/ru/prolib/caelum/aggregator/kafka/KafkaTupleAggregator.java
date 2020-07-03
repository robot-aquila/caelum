package ru.prolib.caelum.aggregator.kafka;

import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;

import org.apache.kafka.streams.kstream.Aggregator;

public class KafkaTupleAggregator implements Aggregator<String, KafkaTuple, KafkaTuple> {

	@Override
	public KafkaTuple apply(String key, KafkaTuple value, KafkaTuple aggregate) {
		if ( aggregate.volume == null ) {
			aggregate.type = value.type;
			aggregate.open = value.open;
			aggregate.high = value.high;
			aggregate.low = value.low;
			aggregate.close = value.close;
			aggregate.volume = value.volume;
			aggregate.bigVolume = value.bigVolume;
			aggregate.decimals = value.decimals;
			aggregate.volDecimals = value.volDecimals;
		} else {
			if ( aggregate.decimals != value.decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
					.append("Value decimals mismatch: symbol: ")
					.append(key)
					.append(", expected: ")
					.append(aggregate.decimals)
					.append(", actual: ")
					.append(value.decimals)
					.toString());
			}
			if ( aggregate.volDecimals != value.volDecimals ) {
				throw new IllegalArgumentException(new StringBuilder()
					.append("Volume decimals mismatch: symbol: ")
					.append(key)
					.append(", expected: ")
					.append(aggregate.volDecimals)
					.append(", actual: ")
					.append(value.volDecimals)
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
	
	@Override
	public int hashCode() {
		return 2890014;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaTupleAggregator.class ) {
			return false;
		}
		return true;
	}

}
