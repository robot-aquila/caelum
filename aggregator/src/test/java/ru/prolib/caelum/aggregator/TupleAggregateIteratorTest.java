package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.core.TupleType;

public class TupleAggregateIteratorTest {
	
	static Long T(String time_string) {
		return Instant.parse(time_string + "Z").toEpochMilli();
	}
	
	static Tuple C(int open, int high, int low, int close, long volume) {
		return new Tuple(open, high, low, close, (byte)2, volume, null, (byte)0, TupleType.LONG_REGULAR);
	}
	
	static KeyValue<Long, Tuple> C(String time_string, int open, int high, int low, int close, long volume) {
		return new KeyValue<>(T(time_string), C(open, high, low, close, volume));
	}
	
	@Rule
	public ExpectedException eex = ExpectedException.none();
	
	WindowStoreIteratorStub<Tuple> source;
	TupleAggregateIterator service;

	@Before
	public void setUp() throws Exception {
		source = new WindowStoreIteratorStub<>(Arrays.asList(
				C("2020-06-04T20:39:00", 150, 180, 145, 141, 10),
				C("2020-06-04T20:40:00", 140, 150, 140, 146, 20),
				C("2020-06-04T20:41:00", 139, 160, 138, 152, 10),
				C("2020-06-04T20:43:00", 149, 152, 147, 151, 30),
				C("2020-06-04T20:44:00", 156, 164, 152, 162, 15),
				C("2020-06-04T20:45:00", 163, 178, 160, 175,  5),
				C("2020-06-04T20:47:00", 178, 181, 174, 179, 10)
			));
		service = new TupleAggregateIterator(source, Duration.ofMinutes(5));
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
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.peekNextKey();
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		service.close();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}

}
