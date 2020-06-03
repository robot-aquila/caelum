package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class LBOHLCVMutableTest {
	LBOHLCVMutable service;
	
	@Before
	public void setUp() throws Exception {
		service = new LBOHLCVMutable();
	}
	
	@Test
	public void testCtor_Default() {
		assertEquals(OHLCVRecordType.LONG_UNKNOWN, service.getType());
		assertEquals(OHLCVRecordType.LONG_UNKNOWN, service.type);
		assertEquals(0L, service.getOpenPrice());
		assertEquals(0L, service.open);
		assertEquals(0L, service.getHighPrice());
		assertEquals(0L, service.high);
		assertEquals(0L, service.getLowPrice());
		assertEquals(0L, service.low);
		assertEquals(0L, service.getClosePrice());
		assertEquals(0L, service.close);
		assertNull(service.volume);
		assertNull(service.getBigVolume());
		assertNull(service.bigVolume);
		assertEquals(0, service.getPriceDecimals());
		assertEquals(0, service.priceDecimals);
		assertEquals(0, service.getVolumeDecimals());
		assertEquals(0, service.volumeDecimals);
	}
	
	@Test
	public void testCtor9() {
		service = new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL);
		
		assertEquals(OHLCVRecordType.LONG_WIDEVOL, service.getType());
		assertEquals(OHLCVRecordType.LONG_WIDEVOL, service.type);
		assertEquals(771L, service.getOpenPrice());
		assertEquals(771L, service.open);
		assertEquals(802L, service.getHighPrice());
		assertEquals(802L, service.high);
		assertEquals(750L, service.getLowPrice());
		assertEquals(750L, service.low);
		assertEquals(799L, service.getClosePrice());
		assertEquals(799L, service.close);
		assertEquals(240L, service.getVolume());
		assertEquals(Long.valueOf(240L), service.volume);
		assertEquals(BigInteger.valueOf(26L), service.getBigVolume());
		assertEquals(BigInteger.valueOf(26L), service.bigVolume);
		assertEquals(5, service.getPriceDecimals());
		assertEquals(5, service.priceDecimals);
		assertEquals(2, service.getVolumeDecimals());
		assertEquals(2, service.volumeDecimals);
	}
	
	@Test
	public void testDirectAccessAndGetters() {
		service.type = OHLCVRecordType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.priceDecimals = 12;
		service.volumeDecimals = 8;
		
		assertEquals(OHLCVRecordType.LONG_WIDEVOL, service.getType());
		assertEquals(1890L, service.getOpenPrice());
		assertEquals(2000L, service.getHighPrice());
		assertEquals(1850L, service.getLowPrice());
		assertEquals(1900L, service.getClosePrice());
		assertEquals(215L, service.getVolume());
		assertEquals(BigInteger.valueOf(756), service.getBigVolume());
		assertEquals(12, service.getPriceDecimals());
		assertEquals( 8, service.getVolumeDecimals());
	}
	
	@Test
	public void testToString() {
		service.type = OHLCVRecordType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.priceDecimals = 12;
		service.volumeDecimals = 8;
		
		String expected = new StringBuilder()
				.append("LBOHLCVMutable[")
				.append("type=LONG_WIDEVOL,")
				.append("open=1890,")
				.append("high=2000,")
				.append("low=1850,")
				.append("close=1900,")
				.append("priceDecimals=12,")
				.append("volume=215,")
				.append("bigVolume=756,")
				.append("volumeDecimals=8")
				.append("]")
				.toString();
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		service.type = OHLCVRecordType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.priceDecimals = 12;
		service.volumeDecimals = 8;
		
		int expected = new HashCodeBuilder(70001237, 701)
				.append(OHLCVRecordType.LONG_WIDEVOL)
				.append(1890L)
				.append(2000L)
				.append(1850L)
				.append(1900L)
				.append((byte)12)
				.append(215L)
				.append(BigInteger.valueOf(756))
				.append((byte)8)
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
		service = new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL);
		
		assertTrue(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(250L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 250L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 250L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 250L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)1,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				250L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(250L), (byte)2,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)1,
				OHLCVRecordType.LONG_WIDEVOL)));
		assertFalse(service.equals(new LBOHLCVMutable(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				OHLCVRecordType.LONG_REGULAR)));
	}

}
