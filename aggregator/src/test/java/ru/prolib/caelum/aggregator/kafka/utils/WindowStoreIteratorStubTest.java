package ru.prolib.caelum.aggregator.kafka.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Test;

public class WindowStoreIteratorStubTest {
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
	public void testHashCode() {
		int expected = new HashCodeBuilder(349617, 71)
				.append(Arrays.asList(
						new KeyValue<>(1000L, 25),
						new KeyValue<>(2000L, 30),
						new KeyValue<>(3000L, 35),
						new KeyValue<>(4000L, 40)
					))
				.append(false) // closed
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings({ "unlikely-arg-type" })
	@Test
	public void testEquals() {
		List<KeyValue<Long, Integer>>
			list1 = Arrays.asList(
				new KeyValue<>(1000L, 25),
				new KeyValue<>(2000L, 30),
				new KeyValue<>(3000L, 35),
				new KeyValue<>(4000L, 40)),
			list2 = Arrays.asList(
				new KeyValue<>(7250L, 10),
				new KeyValue<>(7300L, 11),
				new KeyValue<>(7350L, 12));
		
		assertTrue(service.equals(service));
		assertTrue(service.equals(new WindowStoreIteratorStub<>(list1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new WindowStoreIteratorStub<>(list2)));

		WindowStoreIteratorStub<Integer> x = new WindowStoreIteratorStub<>(list1);
		x.close();
		assertFalse(service.equals(x));
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
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.next());
		assertEquals("Iterator already closed", e.getMessage());
	}

}
