package ru.prolib.caelum.lib.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import ru.prolib.caelum.lib.ItemType;
import ru.prolib.caelum.lib.ByteUtils;

public class KafkaItemDeserializer implements Deserializer<KafkaItem> {
	
	@Override
	public int hashCode() {
		return 219012640;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemDeserializer.class ) {
			return false;
		}
		return true;
	}

	@Override
	public KafkaItem deserialize(String topic, byte[] bytes) {
		byte header = bytes[0];
		switch ( 0b00000011 & header ) {
		case 0:
			throw new IllegalArgumentException("Record type not supported: 0");
		case 1:
			return new KafkaItem(ByteUtils.bytesToLong(bytes, 2, 2), (byte)(bytes[1] & 0x0F),
					(header & 0xFF) >> 2, (byte)((bytes[1] & 0xF0) >> 4),
					ItemType.LONG_COMPACT);
		case 2:
			int value_num_bytes = ((header & 0x1C) >> 2) + 1;
			int volume_num_bytes = ((header & 0xE0) >> 5) + 1;
			return new KafkaItem(ByteUtils.bytesToLong(bytes, 2, value_num_bytes), (byte)(bytes[1] & 0x0F),
					ByteUtils.bytesToLong(bytes, 2 + value_num_bytes, volume_num_bytes), (byte)((bytes[1] & 0xF0) >> 4),
					ItemType.LONG_REGULAR);
		case 3:
			throw new IllegalArgumentException("Record type not supported: 3");
		default:
			throw new IllegalArgumentException("Unidentified header: " + header);
		}
	}

}
