package ru.prolib.caelum.core;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;
import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class TupleTest {
	Tuple service;

	@Before
	public void setUp() throws Exception {
		service = new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3, 100L, null, (byte)2, LONG_REGULAR);
	}
	
	@Test
	public void testGetters() {
		assertEquals("bar", service.getSymbol());
		assertEquals(1578923L, service.getTime());
		assertEquals(300L, service.getOpen());
		assertEquals(320L, service.getHigh());
		assertEquals(290L, service.getLow());
		assertEquals(310L, service.getClose());
		assertEquals((byte)3, service.getDecimals());
		assertEquals(100L, service.getVolume());
		assertEquals(null, service.getBigVolume());
		assertEquals((byte)2, service.getVolumeDecimals());
		assertEquals(LONG_REGULAR, service.getType());
	}
	
	@Test
	public void testGetTimeAsInstant() {
		assertEquals(Instant.ofEpochMilli(1578923L), service.getTimeAsInstant());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("Tuple[symbol=bar,time=1578923,open=300,high=320,low=290,close=310,decimals=3,")
				.append("volume=100,bigVolume=<null>,volumeDecimals=2,type=LONG_REGULAR]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(158993, 109)
				.append("bar")
				.append(1578923L)
				.append(300L)
				.append(320L)
				.append(290L)
				.append(310L)
				.append((byte)3)
				.append(100L)
				.append((BigInteger)null)
				.append((byte)2)
				.append(LONG_REGULAR)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new Tuple("bbb", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1111111L, 300L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 333L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 444L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 555L, 310L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 666L, (byte)3,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)4,
				100L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				111L, null, (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				100L, new BigInteger("123"), (byte)2, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)1, LONG_REGULAR)));
		assertFalse(service.equals(new Tuple("bar", 1578923L, 300L, 320L, 290L, 310L, (byte)3,
				100L, null, (byte)2, LONG_WIDEVOL)));
		assertFalse(service.equals(new Tuple("bbb", 1111111L, 333L, 444L, 555L, 666L, (byte)4,
				111L, new BigInteger("123"), (byte)1, LONG_WIDEVOL)));
	}

	@Test
	public void testOfDecimax15_9() {
		Tuple actual, expected;
		
		actual = Tuple.ofDecimax15("goo", 168479L, 114, 152, 112, 148, 2, 1000, 1);
		expected = new Tuple("goo", 168479L, 114, 152, 112, 148, (byte)2, 1000, null, (byte)1, LONG_REGULAR);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testOfDecimax15_9_ThrowsIfDecimalsGreaterThan15() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Tuple.ofDecimax15("goo", 168479L, 114, 152, 112, 148, 23, 1000, 1));
		assertEquals("Number of decimals must be in range 0-15 but: 23", e.getMessage());
	}
	
	@Test
	public void testOfDecimax15_9_ThrowsIfVolumeDecimalsGreaterThan15() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Tuple.ofDecimax15("goo", 168479L, 114, 152, 112, 148, 2, 1000, 129));
		assertEquals("Number of volume decimals must be in range 0-15 but: 129", e.getMessage());
	}

}
