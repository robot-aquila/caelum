package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

public class KafkaTupleAggregatorTest {
	KafkaTupleAggregator service;
	KafkaTuple aggregate, expected, actual;
	
	@Before
	public void setUp() throws Exception {
		service = new KafkaTupleAggregator();
		aggregate = new KafkaTuple();
	}
	
	@Test
	public void testHashCode() {
		int expected = 2890014;

		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaTupleAggregator()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
	@Test
	public void testApply_FirstValue_Regular() {
		actual = service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_FirstValue_WideVol() {
		actual = service.apply("bar", new KafkaTuple(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		
		expected = new KafkaTuple(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_ThrowsIfValueDecimalsMismatch() {
		service.apply("bar", new KafkaTuple(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> service.apply("bar", new KafkaTuple(1400L, 1405L, 1000L, 1100L, (byte)3,
						1L, null, (byte)10, LONG_REGULAR), aggregate));
		assertEquals("Value decimals mismatch: symbol: bar, expected: 1, actual: 3", e.getMessage());
	}
	
	@Test
	public void testApply_NextValue_ThrowsIfVolumeDecimalsMismatch() {
		service.apply("bar", new KafkaTuple(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> service.apply("bar", new KafkaTuple(1400L, 1405L, 1000L, 1100L, (byte)1,
						1L, null, (byte)7, LONG_REGULAR), aggregate));
		assertEquals("Volume decimals mismatch: symbol: bar, expected: 10, actual: 7", e.getMessage());
	}
	
	@Test
	public void testApply_NextValue_NewHigh() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L, 1350L, 1200L, 1230L, (byte)2,
				500L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new KafkaTuple(1200L, 1350L, 1190L, 1230L, (byte)2,
				600L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertEquals(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_NewLow() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L, 1210L, 1150L, 1240L, (byte)2,
				700L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1150L, 1240L, (byte)2,
				800L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertEquals(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_SwitchedToBigInt_TwoLongs() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				Long.MAX_VALUE, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L, 1210L, 1200L, 1200L, (byte)2,
				Long.MAX_VALUE, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("18446744073709551614"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_SwitchedToBigInt_LongInValue() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				0L, new BigInteger("788891008612"), (byte)0, LONG_WIDEVOL), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L, 1210L, 1200L, 1200L, (byte)2,
				350L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("788891008962"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_SwitchedToBigInt_LongInAggregate() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				12000L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L, 1210L, 1200L, 1200L, (byte)2,
				0L, new BigInteger("80000871200"), (byte)0, LONG_WIDEVOL), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("80000883200"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_SwitchedToBigInt_TwoBigInts() {
		service.apply("bar", new KafkaTuple(1200L, 1250L, 1190L, 1205L, (byte)2,
				0L, new BigInteger("788891008612"), (byte)0, LONG_WIDEVOL), aggregate);
		
		actual = service.apply("bar", new KafkaTuple(1210L,  1210L, 1200L, 1200L, (byte)2,
				0L, new BigInteger("690000126661"), (byte)0, LONG_WIDEVOL), aggregate);
		
		expected = new KafkaTuple(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("1478891135273"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}

}
