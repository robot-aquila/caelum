package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.TupleType;
import ru.prolib.caelum.lib.kafka.KafkaTuple;
import ru.prolib.caelum.service.aggregator.kafka.utils.WindowStoreIteratorStub;

public class KafkaTupleAggregateIteratorTest {
	
	static Long T(String time_string) {
		return Instant.parse(time_string + "Z").toEpochMilli();
	}
	
	static KafkaTuple C(int open, int high, int low, int close, long volume) {
		return new KafkaTuple(open, high, low, close, (byte)2, volume, null, (byte)0, TupleType.LONG_REGULAR);
	}
	
	static KeyValue<Long, KafkaTuple> C(String time_string, int open, int high, int low, int close, long volume) {
		return new KeyValue<>(T(time_string), C(open, high, low, close, volume));
	}
	
	static List<KeyValue<Long, KafkaTuple>> testTuples() {
		return Arrays.asList(
				C("2020-06-04T20:39:00", 150, 180, 145, 141, 10),
				C("2020-06-04T20:40:00", 140, 150, 140, 146, 20),
				C("2020-06-04T20:41:00", 139, 160, 138, 152, 10),
				C("2020-06-04T20:43:00", 149, 152, 147, 151, 30),
				C("2020-06-04T20:44:00", 156, 164, 152, 162, 15),
				C("2020-06-04T20:45:00", 163, 178, 160, 175,  5),
				C("2020-06-04T20:47:00", 178, 181, 174, 179, 10)
			);
	}
	
	static WindowStoreIteratorStub<KafkaTuple> testTuplesIt() {
		return new WindowStoreIteratorStub<>(testTuples());
	}
	
	WindowStoreIteratorStub<KafkaTuple> source;
	KafkaTupleAggregateIterator service;

	@Before
	public void setUp() throws Exception {
		source = testTuplesIt();
		service = new KafkaTupleAggregateIterator(source, Duration.ofMinutes(5));
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(502227, 703)
				.append(source)
				.append(Duration.ofMinutes(5))
				.append(new KafkaTupleAggregator())
				.append(false)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		KafkaTupleAggregateIterator
			service1 = new KafkaTupleAggregateIterator(testTuplesIt(), Duration.ofMinutes(5)),
			service2 = new KafkaTupleAggregateIterator(testTuplesIt(), Duration.ofMinutes(5)), // w.b.closed
			service3 = new KafkaTupleAggregateIterator(testTuplesIt(), Duration.ofMinutes(5)), // w.b.at diff pos
			service4 = new KafkaTupleAggregateIterator(testTuplesIt(), Duration.ofMinutes(1)),
			service5 = new KafkaTupleAggregateIterator(new WindowStoreIteratorStub<>(), Duration.ofMinutes(5));
		service.next(); service.next(); 
		service1.next(); service1.next(); 
		service2.next(); service2.next(); service2.next(); service2.close();
		service3.next(); 
		
		assertTrue(service.equals(service));
		assertTrue(service.equals(service1));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(service2));
		assertFalse(service.equals(service3));
		assertFalse(service.equals(service4));
		assertFalse(service.equals(service5));
	}

	@Test
	public void testIterate() {
		assertEquals(T("2020-06-04T20:35:00"), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(C("2020-06-04T20:35:00", 150, 180, 145, 141, 10), service.next());
		
		assertEquals(T("2020-06-04T20:40:00"), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(C("2020-06-04T20:40:00", 140, 164, 138, 162, 75), service.next());
		
		assertEquals(T("2020-06-04T20:45:00"), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(C("2020-06-04T20:45:00", 163, 181, 160, 179, 15), service.next());
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfClosed() {
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.peekNextKey());
		assertEquals("Iterator already closed", e.getMessage());
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.next());
		assertEquals("Iterator already closed", e.getMessage());
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}

}
