package ru.prolib.caelum.core;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Item implements IItem {
	private final String symbol;
	private final long time, offset;
	private final long value;
	private final byte decimals;
	private final long volume;
	private final byte volumeDecimals;
	private final ItemType type;
	
	public Item(String symbol, long time, long offset, long value, byte decimals,
			long volume, byte volumeDecimals, ItemType type)
	{
		this.symbol = symbol;
		this.time = time;
		this.offset = offset;
		this.value = value;
		this.decimals = decimals;
		this.volume = volume;
		this.volumeDecimals = volumeDecimals;
		this.type = type;
	}
	
	/**
	 * Create an item with zero offset and number of decimals in range of 0-15 for both components.
	 * <p>
	 * @param symbol - symbol
	 * @param time - time in millis since epoch
	 * @param value - item value
	 * @param decimals - number of decimals for value
	 * @param volume - item volume
	 * @param volumeDecimals - number of decimals for volume
	 * @return an item
	 * @throws IllegalArgumentException - if number of decimals are not in allowed range
	 */
	public static Item ofDecimax15(String symbol, long time, long value, int decimals, long volume, int volumeDecimals) {
		ByteUtils utils = ByteUtils.getInstance();
		if ( ! utils.isNumberOfDecimalsFits4Bits(decimals) ) {
			throw new IllegalArgumentException("Number of decimals must be in range 0-15 but: " + decimals);
		}
		if ( ! utils.isNumberOfDecimalsFits4Bits(volumeDecimals) ) {
			throw new IllegalArgumentException("Number of volume decimals must be in range 0-15 but: " + volumeDecimals);
		}
		return new Item(symbol, time, 0L, value, (byte)decimals, volume, (byte)volumeDecimals,
				utils.isLongCompact(value, volume) ? ItemType.LONG_COMPACT : ItemType.LONG_REGULAR);
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
	public long getOffset() {
		return offset;
	}

	@Override
	public Instant getTimeAsInstant() {
		return Instant.ofEpochMilli(time);
	}

	@Override
	public ItemType getType() {
		return type;
	}

	@Override
	public long getValue() {
		return value;
	}

	@Override
	public long getVolume() {
		return volume;
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
				.append("offset", offset)
				.append("value", value)
				.append("decimals", decimals)
				.append("volume", volume)
				.append("volDecimals", volumeDecimals)
				.append("type", type)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(782925, 51)
				.append(symbol)
				.append(time)
				.append(offset)
				.append(value)
				.append(decimals)
				.append(volume)
				.append(volumeDecimals)
				.append(type)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || (other instanceof IItem) == false ) {
			return false;
		}
		IItem o = (IItem) other;
		return new EqualsBuilder()
				.append(o.getSymbol(), symbol)
				.append(o.getTime(), time)
				.append(o.getOffset(), offset)
				.append(o.getValue(), value)
				.append(o.getDecimals(), decimals)
				.append(o.getVolume(), volume)
				.append(o.getVolumeDecimals(), volumeDecimals)
				.append(o.getType(), type)
				.build();
	}

}
