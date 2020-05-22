package ru.prolib.caelum.core;

/**
 * Long type based japanese candlestick definition.
 * Used to store a regular OHLCV data record for long time for fast further aggregation.
 * Symbol and time are stored outside in appropriate Kafka record properties.
 * Number of decimals are limited to 4 bits. In case of oversize an exception will be thrown. *
 */
public class LBCandle {
	private final long open, high, low, close;
	private final byte priceDecimals;
	private final long volume;
	private final byte volumeDecimals;
	/**
	 * A header byte. Used for encoding/decoding to/from binary format. Reserved for future usage.
	 */
	private final Byte header;
	
	/**
	 * This is unsafe constructor - no additional checks performed.
	 * Should be used to restore candlestick only for valid number of decimals.
	 * <p>
	 * @param open - open price
	 * @param high - high price
	 * @param low - low price
	 * @param close - close price
	 * @param price_decimals - number of decimals in price values. Only 4 lower bits are matters.
	 * @param volume - total accumulated volume
	 * @param volume_decimals - number of decimals in volume value. Only 4 lower bits are matters.
	 * @param header - header byte
	 */
	public LBCandle(long open, long high, long low, long close, byte price_decimals,
			long volume, byte volume_decimals, Byte header)
	{
		this.open = open;
		this.close = close;
		this.high = high;
		this.low = low;
		this.volume = volume;
		this.priceDecimals = price_decimals;
		this.volumeDecimals = volume_decimals;
		this.header = header;
	}
	
	public Byte getHeader() {
		return header;
	}
	
	public long getOpenPrice() {
		return open;
	}
	
	public long getHighPrice() {
		return high;
	}
	
	public long getLowPrice() {
		return low;
	}
	
	public long getClosePrice() {
		return close;
	}
	
	public byte getPriceDecimals() {
		return priceDecimals;
	}
	
	public long getVolume() {
		return volume;
	}
	
	public byte getVolumeDecimals() {
		return volumeDecimals;
	}

}
