package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WindowStoreIteratorStubTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	WindowStoreIteratorStub<Integer> service;

	@Before
	public void setUp() throws Exception {
		service = new WindowStoreIteratorStub<>(Arrays.asList(
				new KeyValue<>(1000L, 25),
				new KeyValue<>(2000L, 30),
				new KeyValue<>(3000L, 35),
				new KeyValue<>(4000L, 40)
			));
	}

	@Test
	public void testIterate() {
		assertEquals(Long.valueOf(1000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(new KeyValue<>(1000L, 25), service.next());
		
		assertEquals(Long.valueOf(2000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(new KeyValue<>(2000L, 30), service.next());
		
		assertEquals(Long.valueOf(3000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(new KeyValue<>(3000L, 35), service.next());
		
		assertEquals(Long.valueOf(4000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(new KeyValue<>(4000L, 40), service.next());
		
		assertFalse(service.hasNext());
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfNoMoreData() {
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
