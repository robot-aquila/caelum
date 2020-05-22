package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class LBTradeSerializerTest {
	LBTradeSerializer service;

	@Before
	public void setUp() throws Exception {
		service = new LBTradeSerializer();
	}
	
	@Test
	public void testSerialize_StandardTypeRecord_VolumeLimit() {
		byte expected[] = {
			0x01 | (byte)(0x3F << 2),
			0b00010011,
			(byte)0x5D, (byte)0x94
		};
		
		assertArrayEquals(expected, service.serialize(null, new LBTrade(23956, 3, 63, 1)));
	}
	
	@Test
	public void testSerialize_StandardTypeRecord_PriceLimit() {
		byte expected[] = {
			0x01 | (byte)(0x19 << 2),
			0b00001111,
			(byte)0x7F, (byte)0xFF
		};
		
		assertArrayEquals(expected, service.serialize(null, new LBTrade(Short.MAX_VALUE, 15, 25, 0)));
	}
	
	@Test
	public void testSerialize_SmallExtendedTypeRecord_NegativePrice() {
		byte expected[] = {
			0b00011110, // type 2 | price bytes 8-1 | volume bytes 1-1 -> 000 111 10
			0b00001111,
			(byte)0xFF,(byte)0xE4,(byte)0x53,(byte)0xD5,(byte)0x5F,(byte)0x4B,(byte)0x44,(byte)0x03,
			(byte)0x01
		};
		
		
		assertArrayEquals(expected, service.serialize(null, new LBTrade(-7789123455990781L, 15, 1, 0)));
	}

	@Test
	public void testSerialize_SmallExtendedTypeRecord() {
		byte expected[] = {
			0b01010010, // type 2, price length 5 bytes, volume length 3 bytes
			(byte)0b11110101,
			(byte)0xBE, (byte)0xDB, (byte)0x93, (byte)0xE5, (byte)0xA3, // price
			(byte)0x6E, (byte)0xF8, (byte)0xF0 // volume
		};
		
		assertArrayEquals(expected, service.serialize(null, new LBTrade(819727689123L, 5, 7272688L, 15)));
	}

}
