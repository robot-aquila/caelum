package ru.prolib.caelum.core;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class LBOHLCVMutable implements ILBOHLCV {
	OHLCVRecordType type = OHLCVRecordType.LONG_UNKNOWN;
	long open, high, low, close;
	byte priceDecimals;
	Long volume;
	BigInteger bigVolume;
	byte volumeDecimals;
	
	public LBOHLCVMutable() {
		
	}
	
	public LBOHLCVMutable(long open, long high, long low, long close, byte price_decimals,
			Long volume, BigInteger big_volume, byte volume_decimals, OHLCVRecordType type)
	{
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.priceDecimals = price_decimals;
		this.volume = volume;
		this.bigVolume = big_volume;
		this.volumeDecimals = volume_decimals;
		this.type = type;
	}
	
	@Override
	public OHLCVRecordType getType() {
		return type;
	}

	@Override
	public long getOpenPrice() {
		return open;
	}

	@Override
	public long getHighPrice() {
		return high;
	}

	@Override
	public long getLowPrice() {
		return low;
	}

	@Override
	public long getClosePrice() {
		return close;
	}

	@Override
	public long getVolume() {
		return volume;
	}

	@Override
	public BigInteger getBigVolume() {
		return bigVolume;
	}

	@Override
	public byte getPriceDecimals() {
		return priceDecimals;
	}

	@Override
	public byte getVolumeDecimals() {
		return volumeDecimals;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	@Override
	public int hashCode() {
		return	new HashCodeBuilder(70001237, 701)
				.append(type)
				.append(open)
				.append(high)
				.append(low)
				.append(close)
				.append(priceDecimals)
				.append(volume)
				.append(bigVolume)
				.append(volumeDecimals)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != LBOHLCVMutable.class ) {
			return false;
		}
		LBOHLCVMutable o = (LBOHLCVMutable) other;
		return new EqualsBuilder()
				.append(o.type, type)
				.append(o.open, open)
				.append(o.high, high)
				.append(o.low, low)
				.append(o.close, close)
				.append(o.priceDecimals, priceDecimals)
				.append(o.volume, volume)
				.append(o.bigVolume, bigVolume)
				.append(o.volumeDecimals, volumeDecimals)
				.build();
	}

}
