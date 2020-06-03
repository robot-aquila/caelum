package ru.prolib.caelum.core;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Long type based japanese candlestick definition.
 * Used to store a regular OHLCV data record for long time for fast further aggregation.
 * Symbol and time are stored outside in appropriate Kafka record properties.
 * Number of decimals are limited to 4 bits. In case of oversize an exception will be thrown.
 */
public class LBOHLCV implements ILBOHLCV {
	private final OHLCVRecordType type;
	private final long open, high, low, close;
	private final byte priceDecimals;
	private final long volume;
	private final BigInteger bigVolume;
	private final byte volumeDecimals;
	
	/**
	 * Service constructor.
	 * <p>
	 * @param open - open price
	 * @param high - high price
	 * @param low - low price
	 * @param close - close price
	 * @param price_decimals - number of decimals in price values. Only 4 lower bits are matters.
	 * @param volume - total accumulated volume
	 * @param big_volume - total accumulated volume in case if it's bigger than long
	 * @param volume_decimals - number of decimals in volume value. Only 4 lower bits are matters.
	 * @param type - record type
	 */
	public LBOHLCV(long open, long high, long low, long close, byte price_decimals,
			long volume, BigInteger big_volume, byte volume_decimals,
			OHLCVRecordType type)
	{
		if ( (price_decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Price decimals expected to be in range 0-15 but: " + price_decimals);
		}
		if ( (volume_decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Volume decimals expected to be in range 0-15 but: " + volume_decimals);
		}
		this.open = open;
		this.close = close;
		this.high = high;
		this.low = low;
		this.volume = volume;
		this.bigVolume = big_volume;
		this.priceDecimals = price_decimals;
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
	public byte getPriceDecimals() {
		return priceDecimals;
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
	public byte getVolumeDecimals() {
		return volumeDecimals;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(223007165, 53)
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
		if ( other == null || other.getClass() != LBOHLCV.class ) {
			return false;
		}
		LBOHLCV o = (LBOHLCV) other;
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
