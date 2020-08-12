package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;
import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TupleTest {
	static KafkaTuple tuple1, tuple2;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		tuple1 = new KafkaTuple(200, 240, 200, 225, (byte)3, 0L, new BigInteger("986777881223"), (byte)5, LONG_WIDEVOL);
		tuple2 = new KafkaTuple(150, 180, 120, 130, (byte)2, 1000L, null, (byte)0, LONG_REGULAR);
	}
	
	Tuple service;

	@Before
	public void setUp() throws Exception {
		service = new Tuple("foo@bar", 15782992L, tuple1);
	}
	
	@Test
	public void testCtor3() {
		assertEquals("foo@bar", service.getSymbol());
		assertEquals(15782992L, service.getTime());
		assertEquals(LONG_WIDEVOL, service.getType());
		assertEquals(200L, service.getOpen());
		assertEquals(240L, service.getHigh());
		assertEquals(200L, service.getLow());
		assertEquals(225L, service.getClose());
		assertEquals((byte)3, service.getDecimals());
		assertEquals(0L, service.getVolume());
		assertEquals(new BigInteger("986777881223"), service.getBigVolume());
		assertEquals((byte)5, service.getVolumeDecimals());
	}
	
	@Test
	public void testGetTimeAsInstant() {
		assertEquals(Instant.ofEpochMilli(15782992L), service.getTimeAsInstant());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("Tuple[symbol=foo@bar,time=15782992,tuple=")
				.append("KafkaTuple[type=LONG_WIDEVOL,open=200,high=240,low=200,close=225,")
				.append("decimals=3,volume=0,bigVolume=986777881223,volDecimals=5]")
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(486929107, 95)
				.append("foo@bar")
				.append(15782992L)
				.append(tuple1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals_SpecialCases() {
		assertTrue(service.equals(service));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(new Tuple("foo@bar", 15782992L, tuple1)));
		assertFalse(service.equals(new Tuple("lol@kek", 15782992L, tuple1)));
		assertFalse(service.equals(new Tuple("foo@bar", 11111111L, tuple1)));
		assertFalse(service.equals(new Tuple("foo@bar", 15782992L, tuple2)));
		assertFalse(service.equals(new Tuple("lol@kek", 11111111L, tuple2)));
	}

}
