package ru.prolib.caelum.aggregator.kafka;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.core.TupleType;

public class KafkaTuple {
	public TupleType type = TupleType.LONG_UNKNOWN;
	public long open, high, low, close;
	public byte decimals;
	public Long volume;
	public BigInteger bigVolume;
	public byte volDecimals;
	
	public KafkaTuple() {
		
	}
	
	public KafkaTuple(long open, long high, long low, long close, byte decimals,
			Long volume, BigInteger big_volume, byte vol_decimals, TupleType type)
	{
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.decimals = decimals;
		this.volume = volume;
		this.bigVolume = big_volume;
		this.volDecimals = vol_decimals;
		this.type = type;
	}
	
	public TupleType getType() {
		return type;
	}

	public long getOpen() {
		return open;
	}

	public long getHigh() {
		return high;
	}

	public long getLow() {
		return low;
	}

	public long getClose() {
		return close;
	}

	public long getVolume() {
		return volume;
	}

	public BigInteger getBigVolume() {
		return bigVolume;
	}

	public byte getDecimals() {
		return decimals;
	}

	public byte getVolumeDecimals() {
		return volDecimals;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("type", type)
				.append("open", open)
				.append("high", high)
				.append("low", low)
				.append("close", close)
				.append("decimals", decimals)
				.append("volume", volume)
				.append("bigVolume", bigVolume)
				.append("volDecimals", volDecimals)
				.build();
	}
	
	@Override
	public int hashCode() {
		return	new HashCodeBuilder(70001237, 701)
				.append(type)
				.append(open)
				.append(high)
				.append(low)
				.append(close)
				.append(decimals)
				.append(volume)
				.append(bigVolume)
				.append(volDecimals)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaTuple.class ) {
			return false;
		}
		KafkaTuple o = (KafkaTuple) other;
		return new EqualsBuilder()
				.append(o.type, type)
				.append(o.open, open)
				.append(o.high, high)
				.append(o.low, low)
				.append(o.close, close)
				.append(o.decimals, decimals)
				.append(o.volume, volume)
				.append(o.bigVolume, bigVolume)
				.append(o.volDecimals, volDecimals)
				.build();
	}

}
