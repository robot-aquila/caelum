package ru.prolib.caelum.lib;

import static org.junit.Assert.*;
import static ru.prolib.caelum.lib.ByteUtils.hexStringToByteArr;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

public class ByteUtilsTest {
	
	@Test
	public void testIsLongCompact() {
		assertFalse(ByteUtils.isLongCompact(-251,  13));
		assertFalse(ByteUtils.isLongCompact( 251, -13));
		assertFalse(ByteUtils.isLongCompact(-251, -13));
		assertFalse(ByteUtils.isLongCompact(655788712L, 13));
		assertFalse(ByteUtils.isLongCompact(251, 9127778992L));
		assertFalse(ByteUtils.isLongCompact(65536, 13));
		assertFalse(ByteUtils.isLongCompact(1238, 64));
		assertFalse(ByteUtils.isLongCompact(250L, 100L));
		assertTrue(ByteUtils.isLongCompact(251, 13));
		assertTrue(ByteUtils.isLongCompact(0, 0));
		assertTrue(ByteUtils.isLongCompact(65535, 63));
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits() {
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits( 0));
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits( 3));
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits( 9));
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits(12));
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits(13));
		assertTrue(ByteUtils.isNumberOfDecimalsFits4Bits(15));
		assertFalse(ByteUtils.isNumberOfDecimalsFits4Bits(16));
		assertFalse(ByteUtils.isNumberOfDecimalsFits4Bits(200));
		assertFalse(ByteUtils.isNumberOfDecimalsFits4Bits(255));
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits_ThrowsIfNegativeDecimals() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> ByteUtils.isNumberOfDecimalsFits4Bits(-13));
		assertEquals("Number of decimals must be in range 0-255 but: -13", e.getMessage());
	}
	
	@Test
	public void testIsNumberOfDecimalsFits4Bits_ThrowsIfGreaterThan255() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> ByteUtils.isNumberOfDecimalsFits4Bits(12000));
		assertEquals("Number of decimals must be in range 0-255 but: 12000", e.getMessage());
	}

	@Test
	public void testLongToByteArray_PositiveValues() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x26,(byte)0x71,(byte)0xFB,(byte)0x8C };
		assertEquals(5, ByteUtils.longToByteArray(0x000000082671FB8CL, bytes));
		assertArrayEquals(expected1, bytes);

		byte expected2[] = { (byte)0x56,(byte)0x2C,(byte)0x15,(byte)0xAE,(byte)0x7F,(byte)0x12,(byte)0xFF,(byte)0x00 };
		assertEquals(8, ByteUtils.longToByteArray(0x562C15AE7F12FF00L, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(1, ByteUtils.longToByteArray(0x0000000000000000L, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x27,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(4, ByteUtils.longToByteArray(0x0000000027000000L, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testLongToBytes_PositiveValues() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("00 00 00 08 26 71 FB 8C"), 3, 5);
		assertEquals(expected, ByteUtils.longToBytes(0x000000082671FB8CL));
		
		expected = new Bytes(hexStringToByteArr("56 2C 15 AE 7F 12 FF 00"), 0, 8);
		assertEquals(expected, ByteUtils.longToBytes(0x562C15AE7F12FF00L));
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00 00 00 00 00"), 7, 1);
		assertEquals(expected, ByteUtils.longToBytes(0x0000000000000000L));
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00 27 00 00 00"), 4, 4);
		assertEquals(expected, ByteUtils.longToBytes(0x0000000027000000L));
	}
	
	@Test
	public void testLongToByteArray_NegativeValues() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x8F };
		assertEquals(1, ByteUtils.longToByteArray(0xFFFFFFFFFFFFFF8FL, bytes)); // -113
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD4,(byte)0x1A,(byte)0xAC };
		assertEquals(3, ByteUtils.longToByteArray(0xFFFFFFFFFFD41AACL, bytes)); // -2876756
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0xFF,(byte)0xFC,(byte)0x73,(byte)0xA0,(byte)0xA4,(byte)0x00,(byte)0xA0,(byte)0x82 };
		assertEquals(7, ByteUtils.longToByteArray(0xFFFC73A0A400A082L, bytes)); // -998766123376510
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0xF2,(byte)0x1E,(byte)0xEE,(byte)0xFF,(byte)0x03,(byte)0xE8,(byte)0xF7,(byte)0xD5 };
		assertEquals(8, ByteUtils.longToByteArray(0xF21EEEFF03E8F7D5L, bytes)); // -1000099288180000811
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testLongToBytes_NegativeValues() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF FF FF FF 8F"), 7, 1);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFFFFFFFFFFFF8FL));
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF FF D4 1A AC"), 5, 3);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFFFFFFFFD41AACL));
		
		expected = new Bytes(hexStringToByteArr("FF FC 73 A0 A4 00 A0 82"), 1, 7);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFC73A0A400A082L));
		
		expected = new Bytes(hexStringToByteArr("F2 1E EE FF 03 E8 F7 D5"), 0, 8);
		assertEquals(expected, ByteUtils.longToBytes(0xF21EEEFF03E8F7D5L));
	}

	@Test
	public void testLongToByteArray_PositiveValues_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0xFF,(byte)0x8F };
		assertEquals(3, ByteUtils.longToByteArray(0x000000000000FF8FL, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0xE0,(byte)0x00,(byte)0x00 };
		assertEquals(4, ByteUtils.longToByteArray(0x0000000000E00000L, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(5, ByteUtils.longToByteArray(0x0000000080000000L, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x00,(byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(8, ByteUtils.longToByteArray(0x0080000000000000L, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testLongToBytes_PositiveValues_WhenHighestBitIsBusy() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00 00 00 FF 8F"), 5, 3);
		assertEquals(expected, ByteUtils.longToBytes(0x000000000000FF8FL));
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00 00 E0 00 00"), 4, 4);
		assertEquals(expected, ByteUtils.longToBytes(0x0000000000E00000L));
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00 80 00 00 00"), 3, 5);
		assertEquals(expected, ByteUtils.longToBytes(0x0000000080000000L));
		
		expected = new Bytes(hexStringToByteArr("00 80 00 00 00 00 00 00"), 0, 8);
		assertEquals(expected, ByteUtils.longToBytes(0x0080000000000000L));
	}
	
	@Test
	public void testLongToByteArray_NegativeValues_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[8];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x00,(byte)0x70 };
		assertEquals(3, ByteUtils.longToByteArray(0xFFFFFFFFFFFF0070L, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x1F,(byte)0xFF,(byte)0xFF };
		assertEquals(4, ByteUtils.longToByteArray(0xFFFFFFFFFF1FFFFFL, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(5, ByteUtils.longToByteArray(0xFFFFFFFF7FFFFFFFL, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0xFF,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(8, ByteUtils.longToByteArray(0xFF7FFFFFFFFFFFFFL, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testLongToBytes_NegativeValues_WhenHighestBitIsBusy() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF FF FF 00 70"), 5, 3);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFFFFFFFFFF0070L));
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF FF 1F FF FF"), 4, 4);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFFFFFFFF1FFFFFL));
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF 7F FF FF FF"), 3, 5);
		assertEquals(expected, ByteUtils.longToBytes(0xFFFFFFFF7FFFFFFFL));
		
		expected = new Bytes(hexStringToByteArr("FF 7F FF FF FF FF FF FF"), 0, 8);
		assertEquals(expected, ByteUtils.longToBytes(0xFF7FFFFFFFFFFFFFL));
	}
	
	@Test
	public void testIntToByteArray_PositiveValues() {
		byte bytes[] = new byte[4];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(1, ByteUtils.intToByteArray(0x00000000, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(4, ByteUtils.intToByteArray(0x7FFFFFFF, bytes));
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0x00,(byte)0x00,(byte)0x7E,(byte)0x2A };
		assertEquals(2, ByteUtils.intToByteArray(0x00007E2A, bytes));
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x00,(byte)0x00,(byte)0xF0,(byte)0x2A };
		assertEquals(3, ByteUtils.intToByteArray(0x0000F02A, bytes));
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testIntToBytes_PositiveValues() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("00 00 00 00"), 3, 1);
		assertEquals(expected, ByteUtils.intToBytes(0x00000000));
		
		expected = new Bytes(hexStringToByteArr("7F FF FF FF"), 0, 4);
		assertEquals(expected, ByteUtils.intToBytes(0x7FFFFFFF));
		
		expected = new Bytes(hexStringToByteArr("00 00 7E 2A"), 2, 2);
		assertEquals(expected, ByteUtils.intToBytes(0x00007E2A));
		
		expected = new Bytes(hexStringToByteArr("00 00 F0 2A"), 1, 3);
		assertEquals(expected, ByteUtils.intToBytes(0x0000F02A));
	}
	
	@Test
	public void testIntToByteArray_NegativeValues() {
		byte bytes[] = new byte[4];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x8F };
		assertEquals(1, ByteUtils.intToByteArray(0xFFFFFF8F, bytes)); // -113
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0xD4,(byte)0x1A,(byte)0xAC };
		assertEquals(3, ByteUtils.intToByteArray(0xFFD41AAC, bytes)); // -2876756
		assertArrayEquals(expected2, bytes);
		
		byte expected3[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF };
		assertEquals(1, ByteUtils.intToByteArray(0xFFFFFFFF, bytes)); // -1
		assertArrayEquals(expected3, bytes);
		
		byte expected4[] = { (byte)0x80,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(4, ByteUtils.intToByteArray(0x80000000, bytes)); // -2147483648
		assertArrayEquals(expected4, bytes);
	}
	
	@Test
	public void testIntToBytes_NegativeValues() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("FF FF FF 8F"), 3, 1);
		assertEquals(expected, ByteUtils.intToBytes(0xFFFFFF8F));
		
		expected = new Bytes(hexStringToByteArr("FF D4 1A AC"), 1, 3);
		assertEquals(expected, ByteUtils.intToBytes(0xFFD41AAC));
		
		expected = new Bytes(hexStringToByteArr("FF FF FF FF"), 3, 1);
		assertEquals(expected, ByteUtils.intToBytes(0xFFFFFFFF));
		
		expected = new Bytes(hexStringToByteArr("80 00 00 00"), 0, 4);
		assertEquals(expected, ByteUtils.intToBytes(0x80000000));
	}
	
	@Test
	public void testIntToByteArray_PositiveValues_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[4];
		
		byte expected1[] = { (byte)0x00,(byte)0x00,(byte)0xFF,(byte)0x8F };
		assertEquals(3, ByteUtils.intToByteArray(0x0000FF8F, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0x00,(byte)0xE0,(byte)0x00,(byte)0x00 };
		assertEquals(4, ByteUtils.intToByteArray(0x00E00000, bytes));
		assertArrayEquals(expected2, bytes);
	}
	
	@Test
	public void testIntToBytes_PositiveValues_WhenHighestBitIsBusy() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("00 00 FF 8F"), 1, 3);
		assertEquals(expected, ByteUtils.intToBytes(0x0000FF8F));
		
		expected = new Bytes(hexStringToByteArr("00 E0 00 00"), 0, 4);
		assertEquals(expected, ByteUtils.intToBytes(0x00E00000));
	}
	
	@Test
	public void testIntToByteArray_NegativeValues_WhenHighestBitIsBusy() {
		byte bytes[] = new byte[4];
		
		byte expected1[] = { (byte)0xFF,(byte)0xFF,(byte)0x00,(byte)0x70 };
		assertEquals(3, ByteUtils.intToByteArray(0xFFFF0070, bytes));
		assertArrayEquals(expected1, bytes);
		
		byte expected2[] = { (byte)0xFF,(byte)0x1F,(byte)0xFF,(byte)0xFF };
		assertEquals(4, ByteUtils.intToByteArray(0xFF1FFFFF, bytes));
		assertArrayEquals(expected2, bytes);
	}
	
	@Test
	public void testIntToBytes_NegativeValues_WhenHighestBitIsBusy() {
		Bytes expected;
		
		expected = new Bytes(hexStringToByteArr("FF FF 00 70"), 1, 3);
		assertEquals(expected, ByteUtils.intToBytes(0xFFFF0070));
		
		expected = new Bytes(hexStringToByteArr("FF 1F FF FF"), 0, 4);
		assertEquals(expected, ByteUtils.intToBytes(0xFF1FFFFF));
	}
	
	@Test
	public void testBytesToLong_PositiveValues() {
		byte source1[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x08,(byte)0x26,(byte)0x71,(byte)0xFB,(byte)0x8C };
		assertEquals(0x000000082671FB8CL, ByteUtils.bytesToLong(source1, 3, 5));
		
		byte source2[] = { (byte)0x56,(byte)0x2C,(byte)0x15,(byte)0xAE,(byte)0x7F,(byte)0x12,(byte)0xFF,(byte)0x00 };
		assertEquals(0x562C15AE7F12FF00L, ByteUtils.bytesToLong(source2, 0, 8));
		
		byte source3[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000000000000L, ByteUtils.bytesToLong(source3, 7, 1));
		
		byte source4[] = { (byte)0x00,(byte)0x00,(byte)0x00,(byte)0x00,(byte)0x27,(byte)0x00,(byte)0x00,(byte)0x00 };
		assertEquals(0x0000000027000000L, ByteUtils.bytesToLong(source4, 4, 4));
	}
	
	@Test
	public void testBytesToLong_NegativeValues() {
		byte source1[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0x8F };
		assertEquals(0xFFFFFFFFFFFFFF8FL, ByteUtils.bytesToLong(source1, 7, 1));
		
		byte source2[] = { (byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD4,(byte)0x1A,(byte)0xAC };
		assertEquals(0xFFFFFFFFFFD41AACL, ByteUtils.bytesToLong(source2, 5, 3));
		
		byte source3[] = { (byte)0xFF,(byte)0xFC,(byte)0x73,(byte)0xA0,(byte)0xA4,(byte)0x00,(byte)0xA0,(byte)0x82 };
		assertEquals(0xFFFC73A0A400A082L, ByteUtils.bytesToLong(source3, 1, 7));
		
		byte source4[] = { (byte)0xF2,(byte)0x1E,(byte)0xEE,(byte)0xFF,(byte)0x03,(byte)0xE8,(byte)0xF7,(byte)0xD5 };
		assertEquals(0xF21EEEFF03E8F7D5L, ByteUtils.bytesToLong(source4, 0, 8));
	}
	
	@Test
	public void testCentsToLong_FromBigDecimal() {
		assertEquals(1312000L, ByteUtils.centsToLong(new BigDecimal("13.12000")));
		assertEquals( 200000L, ByteUtils.centsToLong(new BigDecimal("20.0000")));
		assertEquals(   4000L, ByteUtils.centsToLong(new BigDecimal("4000")));
	}
	
	@Test
	public void testByteArrToHexString3() {
		byte[] source = { (byte)0x05, (byte)0xFF, (byte)0xF0, (byte)0x98, (byte)0xDE };
		
		assertEquals("{FF F0 98}", ByteUtils.byteArrToHexString(source, 1, 3));
	}
	
	@Test
	public void testByteArrToHexString1_ByteArray() {
		byte[] source = { (byte)0x05, (byte)0xFF, (byte)0xF0, (byte)0x98, (byte)0xDE };
		
		assertEquals("{05 FF F0 98 DE}", ByteUtils.byteArrToHexString(source));
	}
	
	@Test
	public void testBytesToHexString_Bytes() {
		byte[] source = { (byte)0x05, (byte)0xFF, (byte)0xF0, (byte)0x98, (byte)0xDE };
		
		assertEquals("{FF F0 98}", ByteUtils.bytesToHexString(new Bytes(source, 1, 3)));
	}
	
	@Test
	public void testHexStringToByteArr() {
		byte[] expected = { (byte)0x05, (byte)0xFF, (byte)0xF0, (byte)0x98, (byte)0xDE };
		
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr("{05 FF F0 98 DE}"));
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr("5 FF F0 98 DE"));
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr("  5  FF  F0  98  DE"));
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr("{05FFF098DE}"));
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr("5FFF098DE"));
		assertArrayEquals(expected, ByteUtils.hexStringToByteArr(" {   5   F F  F  0 9 8  D E} "));
	}
	
	@Test
	public void testHexStringToByteArray() {
		Bytes expected = new Bytes(new byte[]{ (byte)0x05, (byte)0xFF, (byte)0xF0, (byte)0x98, (byte)0xDE }, 0, 5);
		
		assertEquals(expected, ByteUtils.hexStringToBytes("{05 FF F0 98 DE}"));
		assertEquals(expected, ByteUtils.hexStringToBytes("5 FF F0 98 DE"));
		assertEquals(expected, ByteUtils.hexStringToBytes("  5  FF  F0  98  DE"));
		assertEquals(expected, ByteUtils.hexStringToBytes("{05FFF098DE}"));
		assertEquals(expected, ByteUtils.hexStringToBytes("5FFF098DE"));
		assertEquals(expected, ByteUtils.hexStringToBytes(" {   5   F F  F  0 9 8  D E} "));
	}
	
    @Test
    public void testIntSize() {
        for ( int i = 0; i < 1000000; i ++ ) {
            int value = ThreadLocalRandom.current().nextInt();
            Bytes bytes = ByteUtils.intToBytes(value);
            assertEquals("Mismatch for value: " + value, bytes.getLength(), ByteUtils.intSize(value));
        }
    }
    
    @Test
    public void testIntToF3b2() {
        assertEquals((byte)0b00000111, ByteUtils.intToF3b(7, 0));
        assertEquals((byte)0b00001110, ByteUtils.intToF3b(7, 1));
        assertEquals((byte)0b00011100, ByteUtils.intToF3b(7, 2));
        assertEquals((byte)0b00111000, ByteUtils.intToF3b(7, 3));
        assertEquals((byte)0b01110000, ByteUtils.intToF3b(7, 4));
        assertEquals((byte)0b11100000, ByteUtils.intToF3b(7, 5));
        assertEquals((byte)0b00011000, ByteUtils.intToF3b(3, 3));
        assertEquals((byte)0b00010000, ByteUtils.intToF3b(2, 3));
        assertEquals((byte)0b00000000, ByteUtils.intToF3b(0, 0));
        assertEquals((byte)0b00000100, ByteUtils.intToF3b(4, 0));
    }
    
    @Test
    public void testIntToF3b2_ThrowsIfValueIsLessThan0() {
        for ( int i = -1; i > -12; i -- ) {
            var value = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b(value, 0));
            assertEquals("Value out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b2_ThrowsIfValueIsGreaterThan7() {
        for ( int i = 8; i < 20; i ++ ) {
            var value = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b(value, 0));
            assertEquals("Value out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b2_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b(1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b2_ThrowsIfPositionIsGreaterThan5() {
        for ( int i = 6; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b(1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b3() {
        assertEquals((byte)0b11001001, ByteUtils.intToF3b((byte)0b11001001, 0b00000001, 0));
        assertEquals((byte)0b11001001, ByteUtils.intToF3b((byte)0b11001000, 0b00000001, 0));
        assertEquals((byte)0b11001111, ByteUtils.intToF3b((byte)0b11001101, 0b00000111, 0));
        assertEquals((byte)0b11001111, ByteUtils.intToF3b((byte)0b11001010, 0b00000111, 0));
        assertEquals((byte)0b10100010, ByteUtils.intToF3b((byte)0b10111010, 0b00000000, 2));
        assertEquals((byte)0b10100010, ByteUtils.intToF3b((byte)0b10111110, 0b00000000, 2));
        assertEquals((byte)0b00110000, ByteUtils.intToF3b((byte)0b00000000, 0b00000110, 3));
        assertEquals((byte)0b00011010, ByteUtils.intToF3b((byte)0b10111010, 0b00000000, 5));
        assertEquals((byte)0b01011010, ByteUtils.intToF3b((byte)0b10111010, 0b00000010, 5));
        assertEquals((byte)0b11111010, ByteUtils.intToF3b((byte)0b00011010, 0b00000111, 5));
    }
    
    @Test
    public void testIntToF3b3_ThrowsIfValueIsLessThan0() {
        for ( int i = -1; i > -12; i -- ) {
            var value = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b((byte)0, value, 0));
            assertEquals("Value out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b3_ThrowsIfValueIsGreaterThan7() {
        for ( int i = 8; i < 20; i ++ ) {
            var value = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b((byte)0, value, 0));
            assertEquals("Value out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b3_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b((byte)0, 1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testIntToF3b3_ThrowsIfPositionIsGreaterThan5() {
        for ( int i = 6; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.intToF3b((byte)0, 1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testF3bToInt_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.f3bToInt((byte)1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testF3bToInt_ThrowsIfPositionIsGreaterThan5() {
        for ( int i = 6; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.f3bToInt((byte)1, position));
            assertEquals("Position out of range 0-5: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testF3bToInt() {
        assertEquals(7, ByteUtils.f3bToInt((byte)0b10110111, 0));
        assertEquals(7, ByteUtils.f3bToInt((byte)0b01101110, 1));
        assertEquals(7, ByteUtils.f3bToInt((byte)0b01011100, 2));
        assertEquals(7, ByteUtils.f3bToInt((byte)0b01111011, 3));
        assertEquals(7, ByteUtils.f3bToInt((byte)0b11110110, 4));
        assertEquals(7, ByteUtils.f3bToInt((byte)0b11101100, 5));
        assertEquals(3, ByteUtils.f3bToInt((byte)0b01011010, 3));
        assertEquals(2, ByteUtils.f3bToInt((byte)0b11010110, 3));
        assertEquals(0, ByteUtils.f3bToInt((byte)0b01101000, 0));
        assertEquals(4, ByteUtils.f3bToInt((byte)0b10111100, 0));
    }
    
    @Test
    public void testF3bToInt_ComplexTest() {
        byte bytes[] = new byte[1];
        for ( int position = 0; position <= 5; position ++ ) {
            for ( int value = 0; value < 7; value ++ ) {
                ThreadLocalRandom.current().nextBytes(bytes);
                byte mask = ByteUtils.intToF3b(0b00000111, position);
                byte source = bytes[0];
                source |= mask;
                source ^= mask;
                source |= ByteUtils.intToF3b(value, position);
                int actual = ByteUtils.f3bToInt(source, position);
                assertEquals(new StringBuilder()
                        .append("Mismatch for value=").append(value).append(" position=").append(position)
                        .toString(), value, actual);
            }
        }
    }
    
    @Test
    public void testBoolToBit3() {
        assertEquals((byte)0b11011001, ByteUtils.boolToBit((byte)0b11011001, true, 7));
        assertEquals((byte)0b11011001, ByteUtils.boolToBit((byte)0b01011001, true, 7));
        assertEquals((byte)0b01011001, ByteUtils.boolToBit((byte)0b01011001, true, 3));
        assertEquals((byte)0b01011001, ByteUtils.boolToBit((byte)0b01010001, true, 3));
        assertEquals((byte)0b01011001, ByteUtils.boolToBit((byte)0b01011001, true, 0));
        assertEquals((byte)0b01011001, ByteUtils.boolToBit((byte)0b01011000, true, 0));
        assertEquals((byte)0b01010001, ByteUtils.boolToBit((byte)0b01011001, false, 3));
        assertEquals((byte)0b01010001, ByteUtils.boolToBit((byte)0b01010001, false, 3));
        assertEquals((byte)0b10011001, ByteUtils.boolToBit((byte)0b11011001, false, 6));
        assertEquals((byte)0b00011001, ByteUtils.boolToBit((byte)0b00011001, false, 6));
    }
    
    @Test
    public void testBoolToBit3_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class,
                    () -> ByteUtils.boolToBit((byte)0b01101001, true, position)
                );
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testBoolToBit3_ThrowsIfPositionIsGreaterThan7() {
        for ( int i = 8; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class,
                    () -> ByteUtils.boolToBit((byte)0b01001101, true, position)
                );
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testBoolToBit2() {
        assertEquals(0b00000001, ByteUtils.boolToBit(true, 0));
        assertEquals(0b00000010, ByteUtils.boolToBit(true, 1));
        assertEquals(0b00000100, ByteUtils.boolToBit(true, 2));
        assertEquals(0b00001000, ByteUtils.boolToBit(true, 3));
        assertEquals(0b00010000, ByteUtils.boolToBit(true, 4));
        assertEquals(0b00100000, ByteUtils.boolToBit(true, 5));
        assertEquals(0b01000000, ByteUtils.boolToBit(true, 6));
        assertEquals((byte)0b10000000, ByteUtils.boolToBit(true, 7));
        
        for ( int position= 0; position <= 7; position ++ ) {
            assertEquals("Mismatch for position=" + position, 0x00, ByteUtils.boolToBit(false, position));
        }
    }
    
    @Test
    public void testBoolToBit2_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.boolToBit(true, position));
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testBoolToBit2_ThrowsIfPositionIsGreaterThan7() {
        for ( int i = 8; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.boolToBit(true, position));
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testBitToBool() {
        assertTrue(ByteUtils.bitToBool((byte) 0b01101001, 0));
        assertFalse(ByteUtils.bitToBool((byte) 0b01101001, 1));
        assertFalse(ByteUtils.bitToBool((byte) 0b01101001, 2));
        assertTrue(ByteUtils.bitToBool((byte) 0b01101001, 3));
        assertFalse(ByteUtils.bitToBool((byte) 0b01101001, 4));
        assertTrue(ByteUtils.bitToBool((byte) 0b01101001, 5));
        assertTrue(ByteUtils.bitToBool((byte) 0b01101001, 6));
        assertFalse(ByteUtils.bitToBool((byte) 0b01101001, 7));
    }
    
    @Test
    public void testBitToBool_ComplexTest() {
        byte bytes[] = new byte[1];
        for ( int position = 0; position <= 7; position ++ ) {
            ThreadLocalRandom.current().nextBytes(bytes);
            byte mask = ByteUtils.boolToBit(true, position);
            byte source = bytes[0];
            source |= mask;
            source ^= mask;
            assertTrue(new StringBuilder()
                    .append("Mismatch for value=true position=").append(position)
                    .toString(), ByteUtils.bitToBool((byte)(source | mask), position));
            assertFalse(new StringBuilder()
                    .append("Mismatch for value=false position=").append(position)
                    .toString(), ByteUtils.bitToBool(source, position));
        }
    }
    
    @Test
    public void testBitToBool_ThrowsIfPositionIsLessThan0() {
        for ( int i = -1; i > -10; i -- ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.bitToBool((byte) 0, position));
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testBitToBool_ThrowsIfPositionIsGreaterThan7() {
        for ( int i = 8; i < 16; i ++ ) {
            var position = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.bitToBool((byte) 0, position));
            assertEquals("Position out of range 0-7: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testSizeToF3b2() {
        assertEquals((byte) 0b00000101, ByteUtils.sizeToF3b(6, 0));
        assertEquals((byte) 0b00011000, ByteUtils.sizeToF3b(7, 2));
        assertEquals((byte) 0b01100000, ByteUtils.sizeToF3b(4, 5));
    }
    
    @Test
    public void testSizeToF3b2_ThrowsIfValueIsLessThan1() {
        for ( int i = 0; i > -9; i -- ) {
            var size = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.sizeToF3b(size, 0));
            assertEquals("Size out of range 1-8: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testSizeToF3b2_ThrowsIfValueIsGreaterThan8() {
        for ( int i = 9; i < 19; i ++ ) {
            var size = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.sizeToF3b(size, 0));
            assertEquals("Size out of range 1-8: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testSizeToF3b3() {
        assertEquals((byte)0b11001001, ByteUtils.sizeToF3b((byte)0b11001001, 2, 0));
        assertEquals((byte)0b11001001, ByteUtils.sizeToF3b((byte)0b11001000, 2, 0));
        assertEquals((byte)0b11001111, ByteUtils.sizeToF3b((byte)0b11001101, 8, 0));
        assertEquals((byte)0b11001111, ByteUtils.sizeToF3b((byte)0b11001010, 8, 0));
        assertEquals((byte)0b10100010, ByteUtils.sizeToF3b((byte)0b10111010, 1, 2));
        assertEquals((byte)0b10100010, ByteUtils.sizeToF3b((byte)0b10111110, 1, 2));
        assertEquals((byte)0b00110000, ByteUtils.sizeToF3b((byte)0b00000000, 7, 3));
        assertEquals((byte)0b00011010, ByteUtils.sizeToF3b((byte)0b10111010, 1, 5));
        assertEquals((byte)0b01011010, ByteUtils.sizeToF3b((byte)0b10111010, 3, 5));
        assertEquals((byte)0b11111010, ByteUtils.sizeToF3b((byte)0b00011010, 8, 5));
    }
    
    @Test
    public void testSizeToF3b3_ThrowsIfValueIsLessThan1() {
        for ( int i = 0; i > -9; i -- ) {
            var size = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.sizeToF3b((byte)0, size, 0));
            assertEquals("Size out of range 1-8: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testSizeToF3b3_ThrowsIfValueIsGreaterThan8() {
        for ( int i = 9; i < 19; i ++ ) {
            var size = i;
            var e = assertThrows(IllegalArgumentException.class, () -> ByteUtils.sizeToF3b((byte)0, size, 0));
            assertEquals("Size out of range 1-8: " + i, e.getMessage());
        }
    }
    
    @Test
    public void testF3bToSize() {
        assertEquals(6, ByteUtils.f3bToSize((byte) 0b00000101, 0));
        assertEquals(7, ByteUtils.f3bToSize((byte) 0b00011000, 2));
        assertEquals(4, ByteUtils.f3bToSize((byte) 0b01100000, 5));
    }

}
