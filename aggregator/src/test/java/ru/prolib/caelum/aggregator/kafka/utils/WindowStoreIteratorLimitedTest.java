package ru.prolib.caelum.aggregator.kafka.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

public class WindowStoreIteratorLimitedTest {
	
	static KeyValue<Long, Integer> KV(long time, int value) {
		return new KeyValue<>(time, value);
	}
	
	static Long LV(long value) {
		return Long.valueOf(value);
	}
	
	WindowStoreIteratorStub<Integer> source;
	WindowStoreIteratorLimited<Integer> service;

	@Before
	public void setUp() throws Exception {
		source = new WindowStoreIteratorStub<>(Arrays.asList(
				KV(1000L,  1),
				KV(2000L,  3),
				KV(3000L,  5),
				KV(4000L,  7),
				KV(5000L,  9),
				KV(6000L, 11),
				KV(7000L, 13),
				KV(8000L, 15),
				KV(9000L, 17)
			));
		service = new WindowStoreIteratorLimited<>(source, 5);
	}
	
	@Test
	public void testHashCode() {
		service.next();
		service.next(); // emulate passed=2
		
		int expected = new HashCodeBuilder(40991, 57219)
				.append(source)
				.append(5L)
				.append(2L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testEquals() {
		WindowStoreIteratorLimited<Integer>
			service1 = new WindowStoreIteratorLimited<>(new WindowStoreIteratorStub<>(Arrays.asList(
				KV(1000L,  1),
				KV(2000L,  3),
				KV(3000L,  5),
				KV(4000L,  7),
				KV(5000L,  9),
				KV(6000L, 11),
				KV(7000L, 13),
				KV(8000L, 15),
				KV(9000L, 17))), 5),
			service2 = new WindowStoreIteratorLimited<>(new WindowStoreIteratorStub<>(Arrays.asList(
				KV(1000L,  1),
				KV(2000L,  3),
				KV(3000L,  5),
				KV(4000L,  7),
				KV(5000L,  9),
				KV(6000L, 11),
				KV(7000L, 13),
				KV(8000L, 15),
				KV(9000L, 17))), 5),
			service3 = new WindowStoreIteratorLimited<>(new WindowStoreIteratorStub<>(Arrays.asList(
				KV(5000L,  9),
				KV(6000L, 11),
				KV(7000L, 13),
				KV(8000L, 15),
				KV(9000L, 17))), 20);
		service.next(); service.next();
		service1.next(); service1.next();
	
		assertTrue(service.equals(service));
		assertTrue(service.equals(service1));
		assertFalse(service.equals(service2)); // passed mismatch
		assertFalse(service.equals(service3)); // data & limit mismatch
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
	@Test
	public void testIterate() {
		assertTrue(service.hasNext());
		assertEquals(KV(1000L, 1), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KV(2000L, 3), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KV(3000L, 5), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KV(4000L, 7), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KV(5000L, 9), service.next());
		
		assertFalse(service.hasNext());
	}
	
	@Test
	public void testPeekNextKey() {
		assertEquals(LV(1000L), service.peekNextKey());
		service.next();
		assertEquals(LV(2000L), service.peekNextKey());
		service.next();
		assertEquals(LV(3000L), service.peekNextKey());
		service.next();
		assertEquals(LV(4000L), service.peekNextKey());
		service.next();
		assertEquals(LV(5000L), service.peekNextKey());
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.peekNextKey());
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfClosed() {
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.peekNextKey());
		assertEquals("Iterator already closed", e.getMessage());
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
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
}
