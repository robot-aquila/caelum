package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ItemType;

public class KafkaItemDeserializerTest {
	KafkaItemDeserializer service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItemDeserializer();
	}
	
	@Test
	public void testHashCode() {
		int expected = 219012640;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaItemDeserializer()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
	@Test
	public void testDeserialize_LongCompact_Case1() {
		byte source[] = {
				0x01 | (byte)(0x3F << 2),
				0b00010011,
				(byte)0x5D, (byte)0x94
			};
		
		KafkaItem expected = new KafkaItem(23956, (byte)3, 63, (byte)1, ItemType.LONG_COMPACT);
		
		assertEquals(expected, service.deserialize(null, source));
	}
	
	@Test
	public void testDeserialize_LongCompact_Case2() {
		byte source[] = {
				0x01 | (byte)(0x19 << 2),
				0b00001111,
				(byte)0x7F, (byte)0xFF
			};
		
		KafkaItem expected = new KafkaItem(Short.MAX_VALUE, (byte)15, 25, (byte)0, ItemType.LONG_COMPACT);
		
		assertEquals(expected, service.deserialize(null, source));
	}
	
	@Test
	public void testDeserialize_LongRegular_NegativeValue() {
		byte source[] = {
			0b00011010,
			(byte)0b10011111,
			(byte)0xE4,(byte)0x53,(byte)0xD5,(byte)0x5F,(byte)0x4B,(byte)0x44,(byte)0x03,
			(byte)0x01
		};
		
		KafkaItem expected = new KafkaItem(-7789123455990781L, (byte)15, 1, (byte)9, ItemType.LONG_REGULAR);
		
		assertEquals(expected, service.deserialize(null, source));
	}

	@Test
	public void testDeserialize_LongRegular_PositiveValue() {
		byte source[] = {
			0b01010110, // type 2, value length 6 bytes, volume length 3 bytes
			(byte)0b11110101,
			(byte)0x00,(byte)0xBE, (byte)0xDB, (byte)0x93, (byte)0xE5, (byte)0xA3, // value
			(byte)0x6E, (byte)0xF8, (byte)0xF0 // volume
		};
		
		KafkaItem expected = new KafkaItem(819727689123L, (byte)5, 7272688L, (byte)15, ItemType.LONG_REGULAR);

		assertEquals(expected, service.deserialize(null,  source));
	}

}
