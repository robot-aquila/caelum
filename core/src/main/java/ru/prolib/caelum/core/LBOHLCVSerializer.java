package ru.prolib.caelum.core;

import java.math.BigInteger;

import org.apache.kafka.common.serialization.Serializer;

public class LBOHLCVSerializer<T extends ILBOHLCV> implements Serializer<T> {
	private final ByteUtils utils;
	
	public LBOHLCVSerializer(ByteUtils utils) {
		this.utils = utils;
	}
	
	public LBOHLCVSerializer() {
		this(ByteUtils.getInstance());
	}
	
	@Override
	public byte[] serialize(String topic, T candle) {
		BigInteger big_volume = candle.getBigVolume();
		byte volume_bytes[] = null;
		if ( big_volume != null ) {
			volume_bytes = big_volume.toByteArray();
		} else {
			volume_bytes = new byte[8];
		}
		int buffer_used_length = 4;
		long open = candle.getOpenPrice(), high = candle.getHighPrice(), low = candle.getLowPrice(),
				close = candle.getClosePrice(), volume = candle.getVolume();
		// Detect max buffer size: 4 bytes for header + max 4 x 8 for OHLC + length of volume_bytes
		byte buffer[] = new byte[36 + volume_bytes.length], abs_bytes[] = new byte[8], rel_bytes[] = new byte[8];
		buffer[1] = (byte)(candle.getPriceDecimals() | (candle.getVolumeDecimals() << 4));
		// 1) pack open price "as is"
		int abs_num_bytes = utils.longToBytes(open, abs_bytes), rel_num_bytes;
		byte byte2 = (byte)((abs_num_bytes - 1) << 1), byte3;
		System.arraycopy(abs_bytes, 8 - abs_num_bytes, buffer, buffer_used_length, abs_num_bytes);
		buffer_used_length += abs_num_bytes;
		// 2) pack high price by detecting best method
		long relative = open - high;
		abs_num_bytes = utils.longToBytes(high, abs_bytes);
		rel_num_bytes = utils.longToBytes(relative, rel_bytes);
		if ( rel_num_bytes < abs_num_bytes ) {
			System.arraycopy(rel_bytes, 8 - rel_num_bytes, buffer, buffer_used_length, rel_num_bytes);
			buffer[2] = (byte)(byte2 | (rel_num_bytes - 1) << 5 | 0b00010000);
			buffer_used_length += rel_num_bytes;
		} else {
			System.arraycopy(abs_bytes, 8 - abs_num_bytes, buffer, buffer_used_length, abs_num_bytes);
			buffer[2] = (byte)(byte2 | (abs_num_bytes - 1) << 5);
			buffer_used_length += abs_num_bytes;
		}
		// 3) pack low price by detecting best method
		relative = open - low;
		abs_num_bytes = utils.longToBytes(low, abs_bytes);
		rel_num_bytes = utils.longToBytes(relative, rel_bytes);
		if ( rel_num_bytes < abs_num_bytes ) {
			System.arraycopy(rel_bytes, 8 - rel_num_bytes, buffer, buffer_used_length, rel_num_bytes);
			byte3 = (byte)(((rel_num_bytes - 1) << 1) | 0b00000001);
			buffer_used_length += rel_num_bytes;
		} else {
			System.arraycopy(abs_bytes, 8 - abs_num_bytes, buffer, buffer_used_length, abs_num_bytes);
			byte3 = (byte)((abs_num_bytes - 1) << 1);
			buffer_used_length += abs_num_bytes;
		}
		// 4) pack close price detecting best method
		relative = open - close;
		abs_num_bytes = utils.longToBytes(close, abs_bytes);
		rel_num_bytes = utils.longToBytes(relative, rel_bytes);
		if ( rel_num_bytes < abs_num_bytes ) {
			System.arraycopy(rel_bytes, 8 - rel_num_bytes, buffer, buffer_used_length, rel_num_bytes);
			buffer[3] = (byte)(byte3 | (rel_num_bytes - 1) << 5 | 0b00010000);
			buffer_used_length += rel_num_bytes;
		} else {
			System.arraycopy(abs_bytes, 8 - abs_num_bytes, buffer, buffer_used_length, abs_num_bytes);
			buffer[3] = (byte)(byte3 | (abs_num_bytes - 1) << 5);
			buffer_used_length += abs_num_bytes;
		}
		// 5) now pack volume
		if ( big_volume != null ) {
			// It's a big value
			System.arraycopy(volume_bytes, 0, buffer, buffer_used_length, volume_bytes.length);
			buffer[0] = (byte)(0b00011110);
			buffer_used_length += volume_bytes.length;
		} else {
			abs_num_bytes = utils.longToBytes(volume, abs_bytes);
			System.arraycopy(abs_bytes, 8 - abs_num_bytes, buffer, buffer_used_length, abs_num_bytes);
			buffer[0] = (byte)(0x02 | (abs_num_bytes - 1) << 2);
			buffer_used_length += abs_num_bytes;
		}
		byte result[] = new byte[buffer_used_length];
		System.arraycopy(buffer, 0, result, 0, buffer_used_length);
		return result;
	}

}
