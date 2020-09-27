package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.TupleType;

public class KafkaTupleDeserializerTest {
	KafkaTupleDeserializer service;
	
	@Before
	public void setUp() throws Exception {
		service = new KafkaTupleDeserializer();
	}
	
	@Test
	public void testHashCode() {
		int expected = 720091;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaTupleDeserializer()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
	@Test
	public void testDeserialize_SimpleCase_AllAbsoluteAndSmallVolume() {
		byte bytes[] = {
				(byte)(0x02 | 0x02 << 2), // type 0x02, volume bytes 3-1=2
				(byte)(0x03 | 0x05 << 4), // value decimals 0x03, volume decimals 0x05
				(byte)(0x02 << 1 | 0x03 << 5), // open bytes 3-1=2, high bytes 4-1=3
				(byte)(0x00 << 1 | 0x01 << 5), // low bytes 1-1=0, close bytes 2-1=1
				(byte)0x18, (byte)0x6A, (byte)0xF5, // open
				(byte)0x02, (byte)0xE6, (byte)0x30, (byte)0x01, // high
				(byte)0x0A, // low
				(byte)0x01, (byte)0xAE, // close
				(byte)0x01, (byte)0x86, (byte)0xA0 // volume
			};
		
		KafkaTuple expected = new KafkaTuple(1600245L, 48640001L, 10L, 430L, (byte)3,
				100000L, null, (byte)5, TupleType.LONG_REGULAR);
		assertEquals(expected, service.deserialize(null, bytes));
	}
	
	@Test
	public void testDeserialize_AllRelativeAndBigVolume() {
		byte bytes[] = {
				(byte)(0x02 | 0x07 << 2), // type 0x02, volume is max (13 bytes length)
				(byte)(0x0A | 0x05 << 4), // value decimals 10, volume decimals 5
				(byte)(0x05 << 1 | 0x01 << 5 | 0b00010000), // open bytes 6-1=5, high is relative -2554 is 2-1=1 bytes
				(byte)(0x00 << 1 | 0x02 << 5 | 0b00010001), // low is rel. -20 is 1-1=0, close is rel. 70930 3-1=2
				(byte)0x10, (byte)0x1F, (byte)0x4C, (byte)0xC9, (byte)0x2B, (byte)0xF9, // open
				(byte)0xF6, (byte)0x06, // high
				(byte)0xEC, // low
				(byte)0x01, (byte)0x15, (byte)0x12, // close
				(byte)0x0C, (byte)0xA1, (byte)0xB0, (byte)0xD2, (byte)0x20, (byte)0xDF, (byte)0x35,
				(byte)0x76, (byte)0x4C, (byte)0x30, (byte)0xED, (byte)0x47, (byte)0xBE 
			};
		
		KafkaTuple expected = new KafkaTuple(17726618283001L, 17726618285555L, 17726618283021L, 17726618212071L, (byte)10,
				0L, new BigInteger("1000778800000001886620000012222"), (byte)5, TupleType.LONG_WIDEVOL);
		assertEquals(expected, service.deserialize(null, bytes));
	}

	@Test
	public void testDeserialize_HalfRelativeAndSmallVolume() {
		byte bytes[] = {
				(byte)(0x02 | 0x00 << 2), // type 0x02, volume is 1-1=0
				(byte)(0x0E | 0x02 << 4), // value decimals 14, volume decimals 2
				(byte)(0x05 << 1 | 0x02 << 5 | 0b00010000), // open bytes 6-1=5, high is 81980 (0x01403C) rel 3-1=2
				(byte)(0x01 << 2 | 0x01 << 5 | 0b00000000), // low is abs. (0xFE8EC6) is 3-1=2, close is abs FDB6 2-1=1
				(byte)0x10, (byte)0x1F, (byte)0x4C, (byte)0xC9, (byte)0x2B, (byte)0xF9,
				(byte)0x01, (byte)0x40, (byte)0x3C,
				(byte)0xFE, (byte)0x8E, (byte)0xC6,
				(byte)0xFD, (byte)0xB6,
				(byte)0x01,
			};
		
		KafkaTuple expected = new KafkaTuple(17726618283001L, 17726618201021L, -94522L, -586L, (byte)14,
				1L, null, (byte)2, TupleType.LONG_REGULAR);
		assertEquals(expected, service.deserialize(null, bytes));
	}

}
