package ru.prolib.caelum.core;

import org.apache.kafka.common.serialization.Deserializer;

public class LBTradeDeserializer implements Deserializer<LBTrade> {
	private final ByteUtils utils;
	
	public LBTradeDeserializer(ByteUtils utils) {
		this.utils = utils;
	}
	
	public LBTradeDeserializer() {
		this(new ByteUtils());
	}

	@Override
	public LBTrade deserialize(String topic, byte[] bytes) {
		byte header = bytes[0];
		switch ( 0b00000011 & header ) {
		case 0:
			throw new IllegalArgumentException("Record type not supported: 0");
		case 1:
			return new LBTrade(utils.bytesToLong(bytes, 2, 2), (byte)(bytes[1] & 0x0F),
					(header & 0xFF) >> 2, (byte)((bytes[1] & 0xF0) >> 4),
					TradeRecordType.LONG_COMPACT);
		case 2:
			int price_num_bytes = ((header & 0x1C) >> 2) + 1;
			int volume_num_bytes = ((header & 0xE0) >> 5) + 1;
			return new LBTrade(utils.bytesToLong(bytes, 2, price_num_bytes), (byte)(bytes[1] & 0x0F),
					utils.bytesToLong(bytes, 2 + price_num_bytes, volume_num_bytes), (byte)((bytes[1] & 0xF0) >> 4),
					TradeRecordType.LONG_REGULAR);
		case 3:
			throw new IllegalArgumentException("Record type not supported: 3");
		default:
			throw new IllegalArgumentException("Unidentified header: " + header);
		}
	}

}
