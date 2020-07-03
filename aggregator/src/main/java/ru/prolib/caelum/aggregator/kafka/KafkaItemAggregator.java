package ru.prolib.caelum.aggregator.kafka;

import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;

import org.apache.kafka.streams.kstream.Aggregator;

import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class KafkaItemAggregator implements Aggregator<String, KafkaItem, KafkaTuple> {

	@Override
	public KafkaTuple apply(String key, KafkaItem value, KafkaTuple aggregate) {
		long item_value = value.getValue(), item_volume = value.getVolume();
		byte item_value_decimals = value.getDecimals(), item_volume_decimals = value.getVolumeDecimals();
		if ( aggregate.volume == null ) {
			aggregate.type = LONG_REGULAR;
			aggregate.open = aggregate.high = aggregate.low = aggregate.close = item_value;
			aggregate.volume = item_volume;
			aggregate.decimals = item_value_decimals;
			aggregate.volDecimals = item_volume_decimals;
		} else {
			if ( aggregate.decimals != item_value_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Value decimals mismatch: symbol: ")
						.append(key)
						.append(", expected: ")
						.append(aggregate.decimals)
						.append(", actual: ")
						.append(item_value_decimals)
						.toString());
			}
			if ( aggregate.volDecimals != item_volume_decimals ) {
				throw new IllegalArgumentException(new StringBuilder()
						.append("Volume decimals mismatch: symbol: ")
						.append(key)
						.append(", expected: ")
						.append(aggregate.volDecimals)
						.append(", actual: ")
						.append(item_volume_decimals)
						.toString());
			}
			if ( aggregate.bigVolume == null ) {
				try {
					aggregate.volume = Math.addExact(aggregate.volume, item_volume);
				} catch ( ArithmeticException e ) {
					aggregate.bigVolume = BigInteger.valueOf(aggregate.volume).add(BigInteger.valueOf(item_volume));
					aggregate.type = LONG_WIDEVOL;
					aggregate.volume = 0L;
				}
			} else {
				aggregate.bigVolume = aggregate.bigVolume.add(BigInteger.valueOf(item_volume));
			}
			
			if ( item_value > aggregate.high ) {
				aggregate.high = item_value;
			}
			if ( item_value < aggregate.low ) {
				aggregate.low = item_value;
			}
			aggregate.close = item_value;
		}
		return aggregate;
	}

}
