package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ByteUtilsTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	ByteUtils service;
	
	@Before
	public void setUp() throws Exception {
		service = new ByteUtils();
	}
	
	@Test
	public void testIsLongCompact() {
		assertFalse(service.isLongCompact(-251,  13));
		assertFalse(service.isLongCompact( 251, -13));
		assertFalse(service.isLongCompact(-251, -13));
		assertFalse(service.isLongCompact(655788712L, 13));
		assertFalse(service.isLongCompact(251, 9127778992L));
		assertFalse(service.isLongCompact(65536, 13));
		assertFalse(service.isLongCompact(1238, 64));
		assertFalse(service.isLongCompact(250L, 100L));
		assertTrue(service.isLongCompact(251, 13));
		assertTrue(service.isLongCompact(0, 0));
		assertTrue(service.isLongCompact(65535, 63));
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits() {
		assertTrue(service.isNumberOfDecimalsFits4Bits( 0));
		assertTrue(service.isNumberOfDecimalsFits4Bits( 3));
		assertTrue(service.isNumberOfDecimalsFits4Bits( 9));
		assertTrue(service.isNumberOfDecimalsFits4Bits(12));
		assertTrue(service.isNumberOfDecimalsFits4Bits(13));
		assertTrue(service.isNumberOfDecimalsFits4Bits(15));
		assertFalse(service.isNumberOfDecimalsFits4Bits(16));
		assertFalse(service.isNumberOfDecimalsFits4Bits(200));
		assertFalse(service.isNumberOfDecimalsFits4Bits(255));
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits_ThrowsIfNegativeDecimals() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Number of decimals must be in range 0-255 but: -13");
		
		service.isNumberOfDecimalsFits4Bits(-13);
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits_ThrowsIfGreaterThan255() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Number of decimals must be in range 0-255 but: 12000");
		
		service.isNumberOfDecimalsFits4Bits(12000);
	}

	@Test
	public void testLongToBytes_PositiveValues() {
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
	public void testLongToBytes_NegativeValues() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x8F };
		assertEquals(1, service.longToBytes(0xFFFFFFFFFFFFFF8FL, bytes)); // -113
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD4,(byte)0x1A,(byte)0xAC };
		assertEquals(3, service.longToBytes(0xFFFFFFFFFFD41AACL, bytes)); // -2876756
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0xFF,(byte)0xFC,(byte)0x73,(byte)0xA0,(byte)0xA4,(byte)0x00,(byte)0xA0,(byte)0x82 };
		assertEquals(7, service.longToBytes(0xFFFC73A0A400A082L, bytes)); // -998766123376510
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0xF2,(byte)0x1E,(byte)0xEE,(byte)0xFF,(byte)0x03,(byte)0xE8,(byte)0xF7,(byte)0xD5 };
		assertEquals(8, service.longToBytes(0xF21EEEFF03E8F7D5L, bytes)); // -1000099288180000811
		assertArrayEquals(expected4, bytes);
	}

	@Test
	public void testLongToBytes_PositiveValue_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0xFF,(byte)0x8F };
		assertEquals(3, service.longToBytes(0x000000000000FF8FL, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0xE0,(byte)0x00,(byte)0x00 };
		assertEquals(4, service.longToBytes(0x0000000000E00000L, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(5, service.longToBytes(0x0000000080000000L, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(8, service.longToBytes(0x0080000000000000L, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testLongToBytes_NegativeValues_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x00,(byte)0x70 };
		assertEquals(3, service.longToBytes(0xFFFFFFFFFFFF0070L, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x1F,(byte)0xFF,(byte)0xFF };
		assertEquals(4, service.longToBytes(0xFFFFFFFFFF1FFFFFL, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(5, service.longToBytes(0xFFFFFFFF7FFFFFFFL, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0xFF,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(8, service.longToBytes(0xFF7FFFFFFFFFFFFFL, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testBytesToLong_PositiveValues() {
		byte source1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x26,(byte)0x71,(byte)0xFB,(byte)0x8C };
		assertEquals(0x000000082671FB8CL, service.bytesToLong(source1, 3, 5));
		
		byte source2[] = { (byte)0x56,(byte)0x2C,(byte)0x15,(byte)0xAE,(byte)0x7F,(byte)0x12,(byte)0xFF,(byte)0x00 };
		assertEquals(0x562C15AE7F12FF00L, service.bytesToLong(source2, 0, 8));
		
		byte source3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000000000000L, service.bytesToLong(source3, 7, 1));
		
		byte source4[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x27,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000027000000L, service.bytesToLong(source4, 4, 4));
	}
	
	@Test
	public void testBytesToLong_NegativeValues() {
		byte source1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x8F };
		assertEquals(0xFFFFFFFFFFFFFF8FL, service.bytesToLong(source1, 7, 1));
		
		byte source2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD4,(byte)0x1A,(byte)0xAC };
		assertEquals(0xFFFFFFFFFFD41AACL, service.bytesToLong(source2, 5, 3));
		
		byte source3[] = { (byte)0xFF,(byte)0xFC,(byte)0x73,(byte)0xA0,(byte)0xA4,(byte)0x00,(byte)0xA0,(byte)0x82 };
		assertEquals(0xFFFC73A0A400A082L, service.bytesToLong(source3, 1, 7));
		
		byte source4[] = { (byte)0xF2,(byte)0x1E,(byte)0xEE,(byte)0xFF,(byte)0x03,(byte)0xE8,(byte)0xF7,(byte)0xD5 };
		assertEquals(0xF21EEEFF03E8F7D5L, service.bytesToLong(source4, 0, 8));
	}

}
