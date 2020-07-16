package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.TupleType;

public class KafkaTupleTest {
	KafkaTuple service;
	
	@Before
	public void setUp() throws Exception {
		service = new KafkaTuple();
	}
	
	@Test
	public void testCtor_Default() {
		assertEquals(TupleType.LONG_UNKNOWN, service.getType());
		assertEquals(TupleType.LONG_UNKNOWN, service.type);
		assertEquals(0L, service.getOpen());
		assertEquals(0L, service.open);
		assertEquals(0L, service.getHigh());
		assertEquals(0L, service.high);
		assertEquals(0L, service.getLow());
		assertEquals(0L, service.low);
		assertEquals(0L, service.getClose());
		assertEquals(0L, service.close);
		assertNull(service.volume);
		assertNull(service.getBigVolume());
		assertNull(service.bigVolume);
		assertEquals(0, service.getDecimals());
		assertEquals(0, service.decimals);
		assertEquals(0, service.getVolumeDecimals());
		assertEquals(0, service.volDecimals);
	}
	
	@Test
	public void testCtor9() {
		service = new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL);
		
		assertEquals(TupleType.LONG_WIDEVOL, service.getType());
		assertEquals(TupleType.LONG_WIDEVOL, service.type);
		assertEquals(771L, service.getOpen());
		assertEquals(771L, service.open);
		assertEquals(802L, service.getHigh());
		assertEquals(802L, service.high);
		assertEquals(750L, service.getLow());
		assertEquals(750L, service.low);
		assertEquals(799L, service.getClose());
		assertEquals(799L, service.close);
		assertEquals(240L, service.getVolume());
		assertEquals(Long.valueOf(240L), service.volume);
		assertEquals(BigInteger.valueOf(26L), service.getBigVolume());
		assertEquals(BigInteger.valueOf(26L), service.bigVolume);
		assertEquals(5, service.getDecimals());
		assertEquals(5, service.decimals);
		assertEquals(2, service.getVolumeDecimals());
		assertEquals(2, service.volDecimals);
	}
	
	@Test
	public void testDirectAccessAndGetters() {
		service.type = TupleType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.decimals = 12;
		service.volDecimals = 8;
		
		assertEquals(TupleType.LONG_WIDEVOL, service.getType());
		assertEquals(1890L, service.getOpen());
		assertEquals(2000L, service.getHigh());
		assertEquals(1850L, service.getLow());
		assertEquals(1900L, service.getClose());
		assertEquals(215L, service.getVolume());
		assertEquals(BigInteger.valueOf(756), service.getBigVolume());
		assertEquals(12, service.getDecimals());
		assertEquals( 8, service.getVolumeDecimals());
	}
	
	@Test
	public void testToString() {
		service.type = TupleType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.decimals = 12;
		service.volDecimals = 8;
		
		String expected = new StringBuilder()
				.append("KafkaTuple[")
				.append("type=LONG_WIDEVOL,")
				.append("open=1890,")
				.append("high=2000,")
				.append("low=1850,")
				.append("close=1900,")
				.append("decimals=12,")
				.append("volume=215,")
				.append("bigVolume=756,")
				.append("volDecimals=8")
				.append("]")
				.toString();
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		service.type = TupleType.LONG_WIDEVOL;
		service.open = 1890L;
		service.high = 2000L;
		service.low  = 1850L;
		service.close= 1900L;
		service.volume = 215L;
		service.bigVolume = BigInteger.valueOf(756);
		service.decimals = 12;
		service.volDecimals = 8;
		
		int expected = new HashCodeBuilder(70001237, 701)
				.append(TupleType.LONG_WIDEVOL)
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
		service = new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL);
		
		assertTrue(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(250L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 250L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 250L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 250L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)1,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				250L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(250L), (byte)2,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)1,
				TupleType.LONG_WIDEVOL)));
		assertFalse(service.equals(new KafkaTuple(771L, 802L, 750L, 799L, (byte)5,
				240L, BigInteger.valueOf(26L), (byte)2,
				TupleType.LONG_REGULAR)));
	}

}
