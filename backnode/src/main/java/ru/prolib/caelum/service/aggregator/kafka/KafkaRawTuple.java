package ru.prolib.caelum.service.aggregator.kafka;

import static ru.prolib.caelum.lib.ByteUtils.bytesToHexString;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import ru.prolib.caelum.lib.Bytes;

public class KafkaRawTuple {
	private final Bytes open, high, low, close, volume;
	private final int decimals, volumeDecimals;
	
	public KafkaRawTuple(
			Bytes open,
			Bytes high,
			Bytes low,
			Bytes close,
			int decimals,
			Bytes volume,
			int volumeDecimals)
	{
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.decimals = decimals;
		this.volume = volume;
		this.volumeDecimals = volumeDecimals;
	}
	
	public Bytes getOpen() {
		return open;
	}
	
	public Bytes getHigh() {
		return high;
	}
	
	public Bytes getLow() {
		return low;
	}
	
	public Bytes getClose() {
		return close;
	}
	
	public Bytes getVolume() {
		return volume;
	}
	
	public int getDecimals() {
		return decimals;
	}
	
	public int getVolumeDecimals() {
		return volumeDecimals;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(56771, 303)
				.append(open)
				.append(high)
				.append(low)
				.append(close)
				.append(volume)
				.append(decimals)
				.append(volumeDecimals)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("open", bytesToHexString(open))
				.append("high", bytesToHexString(high))
				.append("low", bytesToHexString(low))
				.append("close", bytesToHexString(close))
				.append("decimals", decimals)
				.append("volume", bytesToHexString(volume))
				.append("volumeDecimals", volumeDecimals)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaRawTuple.class ) {
			return false;
		}
		KafkaRawTuple o = (KafkaRawTuple) other;
		return new EqualsBuilder()
				.append(open, o.open)
				.append(high, o.high)
				.append(low, o.low)
				.append(close, o.close)
				.append(decimals,  o.decimals)
				.append(volume, o.volume)
				.append(volumeDecimals, o.volumeDecimals)
				.build();
	}
	
}
