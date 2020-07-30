package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.TupleType.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class KafkaItemAggregatorTest {
	KafkaItemAggregator service;
	KafkaTuple aggregate, expected, actual;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItemAggregator();
		aggregate = new KafkaTuple();
	}
	
	@Test
	public void testApply_First() {
		actual = service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		
		expected = new KafkaTuple(45000L, 45000L, 45000L, 45000L, (byte)2,
				10L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_ThrowsIfValueDecimalsMismatch() {
		service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> service.apply("foo", new KafkaItem(49000L, 5, 10, 0), aggregate));
		assertEquals("Value decimals mismatch: symbol: foo, expected: 2, actual: 5", e.getMessage());
	}
	
	@Test
	public void testApply_Next_ThrowsIfVolumeDecimalsMismatch() {
		service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> service.apply("foo", new KafkaItem(49000L, 2, 10, 2), aggregate));
		assertEquals("Volume decimals mismatch: symbol: foo, expected: 0, actual: 2", e.getMessage());
	}
	
	@Test
	public void testApply_Next_NewHigh() {
		service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		
		actual = service.apply("foo", new KafkaItem(49000L, 2, 1, 0), aggregate);
		
		expected = new KafkaTuple(45000L, 49000L, 45000L, 49000L, (byte)2,
				11L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_NewLow() {
		service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		service.apply("foo", new KafkaItem(49000L, 2, 1, 0), aggregate);
		
		actual = service.apply("foo", new KafkaItem(43900L, 2, 5, 0), aggregate);
		
		expected = new KafkaTuple(45000L, 49000L, 43900L, 43900L, (byte)2,
				16L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_Next_SwitchedToBigInt() {
		service.apply("foo", new KafkaItem(45000L, 2, 10, 0), aggregate);
		service.apply("foo", new KafkaItem(49000L, 2,  1, 0), aggregate);
		service.apply("foo", new KafkaItem(43900L, 2,  5, 0), aggregate);

		actual = service.apply("foo", new KafkaItem(46000L, 2, Long.MAX_VALUE, 0), aggregate);
		
		expected = new KafkaTuple(45000L, 49000L, 43900L, 46000L, (byte)2,
				0L, new BigInteger("9223372036854775823"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}

}
