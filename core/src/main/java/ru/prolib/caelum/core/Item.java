package ru.prolib.caelum.core;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Item {
	private final ItemType type;
	private final long value;
	private final byte decimals;
	private final long volume;
	private final byte volDecimals;
	
	/**
	 * Service constructor when restored from binary form.
	 * <p>
	 * @param value - value
	 * @param decimals - number of decimals in value
	 * @param volume - volume value
	 * @param vol_decimals - number of decimals in volume value
	 * @param type - type of source record
	 */
	public Item(long value, byte decimals, long volume, byte vol_decimals, ItemType type) {
		if ( (decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Value decimals expected to be in range 0-15 but: " + decimals);
		}
		if ( (vol_decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Volume decimals expected to be in range 0-15 but: " + vol_decimals);
		}
		this.type = type;
		this.value = value;
		this.volume = volume;
		this.decimals = decimals;
		this.volDecimals = vol_decimals;
	}
	
	public Item(long value, int decimals, long volume, int vol_decimals) {
		this(value, (byte)decimals, volume, (byte)vol_decimals, ItemType.LONG_UNKNOWN);
	}

	public ItemType getType() {
		return type;
	}
	
	public long getValue() {
		return value;
	}
	
	public long getVolume() {
		return volume;
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
				.append("value", value)
				.append("decimals", decimals)
				.append("volume", volume)
				.append("volDecimals", volDecimals)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7182891, 45)
				.append(value)
				.append(decimals)
				.append(volume)
				.append(volDecimals)
				.append(type)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Item.class ) {
			return false;
		}
		Item o = (Item) other;
		return new EqualsBuilder()
				.append(o.value, value)
				.append(o.volume, volume)
				.append(o.decimals, decimals)
				.append(o.volDecimals, volDecimals)
				.append(o.type, type)
				.build();
	}
	
}
