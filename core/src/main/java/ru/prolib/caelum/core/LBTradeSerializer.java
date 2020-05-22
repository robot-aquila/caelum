package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Serializer;

public class LBTradeSerializer implements Serializer<LBTrade> {
	private final ByteUtils utils;
	
	public LBTradeSerializer(ByteUtils utils) {
		this.utils = utils;
	}
	
	public LBTradeSerializer() {
		this(ByteUtils.getInstance());
	}
	
	@Override
	public byte[] serialize(String topic, LBTrade trade) {
		long price = trade.getPrice(), volume = trade.getVolume();
		if ( utils.isLongCompactTrade(price, volume) ) {
			// price in range of two bytes and volume in range of 6 bits
			byte buffer[] = new byte[4];
			buffer[0] = (byte)(0x01 | (volume & 0x3F) << 2);
			buffer[1] = (byte)(trade.getPriceDecimals() | (trade.getVolumeDecimals() << 4));
			buffer[3] = (byte)(0xFF & price);
			price >>= 8;
			buffer[2] = (byte)(0xFF & price);
			return buffer;
		}
		
		byte price_bytes[] = new byte[8], volume_bytes[] = new byte[8];
		int price_num_bytes = utils.longToBytes(price, price_bytes),
			volume_num_bytes = utils.longToBytes(volume, volume_bytes);
		byte buffer[] = new byte[price_num_bytes + volume_num_bytes + 2];
		buffer[0] = (byte)(0x02 | (price_num_bytes - 1) << 2 | (volume_num_bytes - 1) << 5);
		buffer[1] = (byte)(trade.getPriceDecimals() | (trade.getVolumeDecimals() << 4));
		System.arraycopy(price_bytes, 8 - price_num_bytes, buffer, 2, price_num_bytes);
		System.arraycopy(volume_bytes, 8 - volume_num_bytes, buffer, 2 + price_num_bytes, volume_num_bytes);
		return buffer;
	}

}
