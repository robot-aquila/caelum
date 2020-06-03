package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LBOHLCVTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	LBOHLCV service;

	@Before
	public void setUp() throws Exception {
		service = new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL);
	}
	
	@Test
	public void testCtor9() {
		assertEquals(130L, service.getOpenPrice());
		assertEquals(240L, service.getHighPrice());
		assertEquals(100L, service.getLowPrice());
		assertEquals(200L, service.getClosePrice());
		assertEquals((byte)3, service.getPriceDecimals());
		assertEquals(400L, service.getVolume());
		assertEquals(BigInteger.valueOf(450L), service.getBigVolume());
		assertEquals((byte)1, service.getVolumeDecimals());
		assertEquals(OHLCVRecordType.LONG_WIDEVOL, service.getType());
	}
	
	@Test
	public void testCtor9_ThrowsIfPriceDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals expected to be in range 0-15 but: -1");
		
		new LBOHLCV(130L, 240L, 100L, 200L, (byte)-1,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL);
	}
	
	@Test
	public void testCtor9_ThrowsIfPriceDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals expected to be in range 0-15 but: 17");
		
		new LBOHLCV(130L, 240L, 100L, 200L, (byte)17,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL);
	}
	
	@Test
	public void testCtor9_ThrowsIfVolumeDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: -3");
		
		new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)-3,
				OHLCVRecordType.LONG_WIDEVOL);
	}
	
	@Test
	public void testCtor9_ThrowsIfVolumeDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: 26");
		
		new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)26,
				OHLCVRecordType.LONG_WIDEVOL);
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("LBOHLCV[")
				.append("type=LONG_WIDEVOL,")
				.append("open=130,")
				.append("high=240,")
				.append("low=100,")
				.append("close=200,")
				.append("priceDecimals=3,")
				.append("volume=400,")
				.append("bigVolume=450,")
				.append("volumeDecimals=1")
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(223007165, 53)
				.append(OHLCVRecordType.LONG_WIDEVOL)
				.append(130L)
				.append(240L)
				.append(100L)
				.append(200L)
				.append((byte)3)
				.append(Long.valueOf(400L))
				.append(BigInteger.valueOf(450))
				.append((byte)1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals_SpecialCases() {
		assertTrue(service.equals(service));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(999L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 999L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 999L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 999L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)9,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				999L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(999L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)9,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCV(130L, 240L, 100L, 200L, (byte)3,
				400L, BigInteger.valueOf(450L), (byte)1,
				OHLCVRecordType.LONG_REGULAR)));
	}

}
