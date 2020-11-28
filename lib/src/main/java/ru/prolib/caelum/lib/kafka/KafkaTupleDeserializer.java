package ru.prolib.caelum.lib.kafka;

import java.math.BigInteger;

import org.apache.kafka.common.serialization.Deserializer;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.TupleType;

public class KafkaTupleDeserializer implements Deserializer<KafkaTuple>  {
	
	@Override
	public int hashCode() {
		return 720091;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if  ( other == null || other.getClass() != KafkaTupleDeserializer.class ) {
			return false;
		}
		return true;
	}
	
	@Override
	public KafkaTuple deserialize(String topic, byte[] bytes) {
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
			// 1) unpack open value. it's always absolute
			long open = ByteUtils.bytesToLong(bytes, offset, open_num_bytes);
			offset += open_num_bytes;
			// 2) unpack high value. possible relative
			long high = ByteUtils.bytesToLong(bytes, offset, high_num_bytes);
			if ( (bytes[2] & 0b00010000) != 0 ) high = open - high;
			offset += high_num_bytes;
			// 3) unpack low value. possible relative
			long low = ByteUtils.bytesToLong(bytes, offset, low_num_bytes);
			if ( (bytes[3] & 0b00000001) != 0 ) low = open - low;
			offset += low_num_bytes;
			// 4) unpack close value. possible relative
			long close = ByteUtils.bytesToLong(bytes, offset, close_num_bytes);
			if ( (bytes[3] & 0b00010000) != 0 ) close = open - close;
			offset += close_num_bytes;
			// 5) unpack volume
			long volume = 0;
			BigInteger big_volume = null;
			TupleType type = TupleType.LONG_REGULAR;
			int actual_volume_num_bytes = bytes.length - offset;
			if ( actual_volume_num_bytes == volume_num_bytes ) {
				volume = ByteUtils.bytesToLong(bytes, offset, actual_volume_num_bytes);
			} else {
				type = TupleType.LONG_WIDEVOL;
				byte big_volume_bytes[] = new byte[actual_volume_num_bytes];
				System.arraycopy(bytes, offset, big_volume_bytes, 0, actual_volume_num_bytes);
				big_volume = new BigInteger(big_volume_bytes);
			}
			return new KafkaTuple(open, high, low, close, (byte)(bytes[1] & 0b00001111),
					volume, big_volume, (byte)((bytes[1] & 0b11110000) >> 4), type);
		}
		case 0:
		case 1:
		case 3:
		default:
			throw new IllegalArgumentException("Record type not supported: " + (0b00000011 & header));
		}
	}

}
