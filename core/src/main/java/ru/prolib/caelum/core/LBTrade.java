package ru.prolib.caelum.core;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Long type based trade definition.
 * Used to store a regular trade for long time for fast further aggregation.
 * Symbol and time are stored outside in appropriate Kafka record properties.
 * Number of decimals are limited to 4 bits. In case of oversize an exception will be thrown.
 */
public class LBTrade implements ILBTrade {
	private final TradeRecordType type;
	private final long price;
	private final byte priceDecimals;
	private final long volume;
	private final byte volumeDecimals;
	
	/**
	 * Service constructor when restored from binary form.
	 * <p>
	 * @param price - price value
	 * @param price_decimals - number of decimals in price value
	 * @param volume - volume value
	 * @param volume_decimals - number of decimals in volume value
	 * @param type - type of source record
	 */
	public LBTrade(long price, byte price_decimals, long volume, byte volume_decimals, TradeRecordType type) {
		if ( (price_decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Price decimals expected to be in range 0-15 but: " + price_decimals);
		}
		if ( (volume_decimals & 0xF0) != 0 ) {
			throw new IllegalArgumentException("Volume decimals expected to be in range 0-15 but: " + volume_decimals);
		}
		this.type = type;
		this.price = price;
		this.volume = volume;
		this.priceDecimals = price_decimals;
		this.volumeDecimals = volume_decimals;
	}
	
	public LBTrade(long price, int price_decimals, long volume, int volume_decimals) {
		this(price, (byte)price_decimals, volume, (byte)volume_decimals, TradeRecordType.LONG_UNKNOWN);
	}

	@Override
	public TradeRecordType getType() {
		return type;
	}
	
	@Override
	public long getPrice() {
		return price;
	}
	
	@Override
	public long getVolume() {
		return volume;
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
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("type", type)
				.append("price", price)
				.append("priceDecimals", priceDecimals)
				.append("volume", volume)
				.append("volumeDecimals", volumeDecimals)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7182891, 45)
				.append(price)
				.append(priceDecimals)
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
		if ( other == null || other.getClass() != LBTrade.class ) {
			return false;
		}
		LBTrade o = (LBTrade) other;
		return new EqualsBuilder()
				.append(o.price, price)
				.append(o.volume, volume)
				.append(o.priceDecimals, priceDecimals)
				.append(o.volumeDecimals, volumeDecimals)
				.append(o.type, type)
				.build();
	}
	
}
