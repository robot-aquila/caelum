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
public class LBTrade {
	private final long price;
	private final byte priceDecimals;
	private final long volume;
	private final byte volumeDecimals;
	/**
	 * A header byte. Used for encoding/decoding to/from binary format. Reserved for future usage.
	 */
	private final Byte header;
	
	/**
	 * This is unsafe constructor - no additional checks performed.
	 * Should be used to restore trade only for valid number of decimals.
	 * <p>
	 * @param price - price value
	 * @param price_decimals - number of decimals in price value
	 * @param volume - volume value
	 * @param volume_decimals - number of decimals in volume value
	 */
	public LBTrade(long price, byte price_decimals, long volume, byte volume_decimals, Byte header) {
		this.header = header;
		this.price = price;
		this.volume = volume;
		this.priceDecimals = price_decimals;
		this.volumeDecimals = volume_decimals;
	}
	
	public LBTrade(long price, int price_decimals, long volume, int volume_decimals) {
		this(price, (byte) price_decimals, volume, (byte) volume_decimals, null);
		if ( price_decimals < 0 || price_decimals > 15 ) {
			throw new IllegalArgumentException("Price decimals expected to be in range 0-15 but: " + price_decimals);
		}
		if ( volume_decimals < 0 || volume_decimals > 15 ) {
			throw new IllegalArgumentException("Volume decimals expected to be in range 0-15 but: " + volume_decimals);
		}
	}
	
	public Byte getHeader() {
		return header;
	}
	
	public long getPrice() {
		return price;
	}
	
	public long getVolume() {
		return volume;
	}
	
	public byte getPriceDecimals() {
		return priceDecimals;
	}
	
	public byte getVolumeDecimals() {
		return volumeDecimals;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7182891, 45)
				.append(price)
				.append(priceDecimals)
				.append(volume)
				.append(volumeDecimals)
				.append(header)
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
				.append(o.header, header)
				.build();
	}
	
}
