package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WindowStoreIteratorLimitedTest {
	
	static KeyValue<Long, Integer> KV(long time, int value) {
		return new KeyValue<>(time, value);
	}
	
	static Long LV(long value) {
		return Long.valueOf(value);
	}
	
	@Rule
	public ExpectedException eex = ExpectedException.none();
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
		eex.expect(NoSuchElementException.class);
		
		service.peekNextKey();
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfClosed() {
		service.close();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.peekNextKey();
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
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
}
