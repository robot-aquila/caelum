package ru.prolib.caelum.lib;

import java.math.BigInteger;
import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Tuple implements ITuple {
	private final String symbol;
	private final long time, open, high, low, close, volume;
	private final byte decimals, volumeDecimals;
	private final BigInteger bigVolume;
	private final TupleType type;
	
	public Tuple(String symbol, long time, long open, long high, long low, long close, byte decimals,
			long volume, BigInteger bigVolume, byte volumeDecimals, TupleType type)
	{
		this.symbol = symbol;
		this.time = time;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.decimals = decimals;
		this.volume = volume;
		this.bigVolume = bigVolume;
		this.volumeDecimals = volumeDecimals;
		this.type = type;
	}
	
	/**
	 * Create a tuple with number of decimals in range of 0-15 for both components.
	 * <p>
	 * @param symbol - symbol
	 * @param time - time
	 * @param open - open value
	 * @param high - high value
	 * @param low - low value
	 * @param close - close value
	 * @param decimals - number of decimals for value
	 * @param volume - volume
	 * @param volumeDecimals - number of decimals for volume
	 * @return a tuple
	 * @throws IllegalArgumentException - if number of decimals are not in allowed range
	 */
	public static Tuple ofDecimax15(String symbol, long time, long open, long high, long low, long close, int decimals,
			long volume, int volumeDecimals)
	{
		if ( ! ByteUtils.isNumberOfDecimalsFits4Bits(decimals) ) {
			throw new IllegalArgumentException("Number of decimals must be in range 0-15 but: " + decimals);
		}
		if ( ! ByteUtils.isNumberOfDecimalsFits4Bits(volumeDecimals) ) {
			throw new IllegalArgumentException("Number of volume decimals must be in range 0-15 but: " + volumeDecimals);
		}
		return new Tuple(symbol, time, open, high, low, close, (byte)decimals,
				volume, null, (byte)volumeDecimals, TupleType.LONG_REGULAR);
	}

	@Override
	public String getSymbol() {
		return symbol;
	}

	@Override
	public long getTime() {
		return time;
	}

	@Override
	public Instant getTimeAsInstant() {
		return Instant.ofEpochMilli(time);
	}

	@Override
	public TupleType getType() {
		return type;
	}

	@Override
	public long getOpen() {
		return open;
	}

	@Override
	public long getHigh() {
		return high;
	}

	@Override
	public long getLow() {
		return low;
	}

	@Override
	public long getClose() {
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
	public byte getDecimals() {
		return decimals;
	}

	@Override
	public byte getVolumeDecimals() {
		return volumeDecimals;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("open", open)
				.append("high", high)
				.append("low", low)
				.append("close", close)
				.append("decimals", decimals)
				.append("volume", volume)
				.append("bigVolume", bigVolume)
				.append("volumeDecimals", volumeDecimals)
				.append("type", type)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(158993, 109)
				.append(symbol)
				.append(time)
				.append(open)
				.append(high)
				.append(low)
				.append(close)
				.append(decimals)
				.append(volume)
				.append(bigVolume)
				.append(volumeDecimals)
				.append(type)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if  ( other == this ) {
			return true;
		}
		if ( other == null || (other instanceof ITuple) == false ) {
			return false;
		}
		ITuple o = (ITuple) other;
		return new EqualsBuilder()
				.append(o.getSymbol(), symbol)
				.append(o.getTime(), time)
				.append(o.getOpen(), open)
				.append(o.getHigh(), high)
				.append(o.getLow(), low)
				.append(o.getClose(), close)
				.append(o.getDecimals(), decimals)
				.append(o.getVolume(), volume)
				.append(o.getBigVolume(), bigVolume)
				.append(o.getVolumeDecimals(), volumeDecimals)
				.append(o.getType(), type)
				.build();
	}

}
