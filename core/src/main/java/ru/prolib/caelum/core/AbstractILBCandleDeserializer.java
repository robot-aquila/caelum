package ru.prolib.caelum.core;

import java.math.BigInteger;

import org.apache.kafka.common.serialization.Deserializer;

public abstract class AbstractILBCandleDeserializer<T extends ILBCandle> implements Deserializer<T>  {
	protected final ByteUtils utils;
	
	public AbstractILBCandleDeserializer(ByteUtils utils) {
		this.utils = utils;
	}
	
	public AbstractILBCandleDeserializer() {
		this(ByteUtils.getInstance());
	}
	
	@Override
	public T deserialize(String topic, byte[] bytes) {
		byte header = bytes[0];
		switch ( 0b00000011 & header ) {
		case 2:
		{
			int volume_num_bytes = ((header & 0b00011100) >> 2) + 1,
				open_num_bytes = ((bytes[2] & 0b00001110) >> 1) + 1,
				high_num_bytes = ((bytes[2] & 0b11100000) >> 5) + 1,
				low_num_bytes = ((bytes[3] & 0b00001110) >> 1) + 1,
				close_num_bytes = ((bytes[3] & 0b11100000) >> 5) + 1,
				offset = 4;
			// 1) unpack open price. it's always absolute
			long open = utils.bytesToLong(bytes, offset, open_num_bytes);
			offset += open_num_bytes;
			// 2) unpack high price. possible relative
			long high = utils.bytesToLong(bytes, offset, high_num_bytes);
			if ( (bytes[2] & 0b00010000) != 0 ) high = open - high;
			offset += high_num_bytes;
			// 3) unpack low price. possible relative
			long low = utils.bytesToLong(bytes, offset, low_num_bytes);
			if ( (bytes[3] & 0b00000001) != 0 ) low = open - low;
			offset += low_num_bytes;
			// 4) unpack close price. possible relative
			long close = utils.bytesToLong(bytes, offset, close_num_bytes);
			if ( (bytes[3] & 0b00010000) != 0 ) close = open - close;
			offset += close_num_bytes;
			// 5) unpack volume
			long volume = 0;
			BigInteger big_volume = null;
			CandleRecordType type = CandleRecordType.LONG_REGULAR;
			int actual_volume_num_bytes = bytes.length - offset;
			if ( actual_volume_num_bytes == volume_num_bytes ) {
				volume = utils.bytesToLong(bytes, offset, actual_volume_num_bytes);
			} else {
				type = CandleRecordType.LONG_WIDEVOL;
				byte big_volume_bytes[] = new byte[actual_volume_num_bytes];
				System.arraycopy(bytes, offset, big_volume_bytes, 0, actual_volume_num_bytes);
				big_volume = new BigInteger(big_volume_bytes);
			}
			return produce(open, high, low, close, (byte)(bytes[1] & 0b00001111),
					volume, big_volume, (byte)((bytes[1] & 0b11110000) >> 4), type);
		}
		case 0:
		case 1:
		case 3:
		default:
			throw new IllegalArgumentException("Record type not supported: " + (0b00000011 & header));
		}
	}
	
	abstract protected T produce(long open, long high, long low, long close, byte price_decimals,
			long volume, BigInteger big_volume, byte volume_decimals, CandleRecordType type);

}
