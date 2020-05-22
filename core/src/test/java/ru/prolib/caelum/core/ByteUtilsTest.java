package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ByteUtilsTest {
	ByteUtils service;
	
	@Before
	public void setUp() throws Exception {
		service = new ByteUtils();
	}

	@Test
	public void testLongToBytes() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x26,(byte)0x71,(byte)0xFB,(byte)0x8C };
		assertEquals(5, service.longToBytes(0x000000082671FB8CL, bytes));
		assertArrayEquals(expected1, bytes);

		byte expected2[] = { (byte)0x56,(byte)0x2C,(byte)0x15,(byte)0xAE,(byte)0x7F,(byte)0x12,(byte)0xFF,(byte)0x00 };
		assertEquals(8, service.longToBytes(0x562C15AE7F12FF00L, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(1, service.longToBytes(0x0000000000000000L, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x27,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(4, service.longToBytes(0x0000000027000000L, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testBytesToLong() {
		byte source1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x26,(byte)0x71,(byte)0xFB,(byte)0x8C };
		assertEquals(0x000000082671FB8CL, service.bytesToLong(source1, 3, 5));
		
		byte source2[] = { (byte)0x56,(byte)0x2C,(byte)0x15,(byte)0xAE,(byte)0x7F,(byte)0x12,(byte)0xFF,(byte)0x00 };
		assertEquals(0x562C15AE7F12FF00L, service.bytesToLong(source2, 0, 8));
		
		byte source3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000000000000L, service.bytesToLong(source3, 7, 1));
		
		byte source4[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x27,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000027000000L, service.bytesToLong(source4, 4, 4));
	}

}
