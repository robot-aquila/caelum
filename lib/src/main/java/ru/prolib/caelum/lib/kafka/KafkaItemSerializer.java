package ru.prolib.caelum.lib.kafka;

import org.apache.kafka.common.serialization.Serializer;

import ru.prolib.caelum.lib.ByteUtils;

public class KafkaItemSerializer implements Serializer<KafkaItem> {
	
	@Override
	public int hashCode() {
		return 55781924;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemSerializer.class ) {
			return false;
		}
		return true;
	}
	
	@Override
	public byte[] serialize(String topic, KafkaItem item) {
		long value = item.getValue(), volume = item.getVolume();
		if ( ByteUtils.isLongCompact(value, volume) ) {
			// value in range of two bytes and volume in range of 6 bits
			byte buffer[] = new byte[4];
			buffer[0] = (byte)(0x01 | (volume & 0x3F) << 2);
			buffer[1] = (byte)(item.getDecimals() | (item.getVolumeDecimals() << 4));
			buffer[3] = (byte)(0xFF & value);
			value >>= 8;
			buffer[2] = (byte)(0xFF & value);
			return buffer;
		}
		
		byte value_bytes[] = new byte[8], volume_bytes[] = new byte[8];
		int value_num_bytes = ByteUtils.longToByteArray(value, value_bytes),
			volume_num_bytes = ByteUtils.longToByteArray(volume, volume_bytes);
		byte buffer[] = new byte[value_num_bytes + volume_num_bytes + 2];
		buffer[0] = (byte)(0x02 | (value_num_bytes - 1) << 2 | (volume_num_bytes - 1) << 5);
		buffer[1] = (byte)(item.getDecimals() | (item.getVolumeDecimals() << 4));
		System.arraycopy(value_bytes, 8 - value_num_bytes, buffer, 2, value_num_bytes);
		System.arraycopy(volume_bytes, 8 - volume_num_bytes, buffer, 2 + value_num_bytes, volume_num_bytes);
		return buffer;
	}

}
