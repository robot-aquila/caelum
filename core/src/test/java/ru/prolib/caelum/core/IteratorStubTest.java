package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

public class IteratorStubTest {
	List<Integer> data1, data2, data3;
	IteratorStub<Integer> service;

	@Before
	public void setUp() throws Exception {
		data1 = new ArrayList<>(Arrays.asList(1, 2, 5, 12, 19));
		data2 = Arrays.asList(1, 2, 5, 12, 19);
		data3 = Arrays.asList(8, 3, 17, 5, 15);
		service = new IteratorStub<>(data1);
	}
	
	@Test
	public void testCtor2() {
		service = new IteratorStub<>(data1, true);
		data1.remove(3); // shouldn't affect the iterator
		data1.remove(2);
		data1.remove(1);
		
		assertEquals(Integer.valueOf( 1), service.next());
		assertEquals(Integer.valueOf( 2), service.next());
		assertEquals(Integer.valueOf( 5), service.next());
		assertEquals(Integer.valueOf(12), service.next());
		assertEquals(Integer.valueOf(19), service.next());
	}
	
	@Test
	public void testIterate() {
		assertTrue(service.hasNext());
		assertEquals(Integer.valueOf( 1), service.next());
		assertTrue(service.hasNext());
		assertEquals(Integer.valueOf( 2), service.next());
		assertTrue(service.hasNext());
		assertEquals(Integer.valueOf( 5), service.next());
		assertTrue(service.hasNext());
		assertEquals(Integer.valueOf(12), service.next());
		assertTrue(service.hasNext());
		assertEquals(Integer.valueOf(19), service.next());
		assertFalse(service.hasNext());
	}
	
	@Test
	public void testClosed() {
		assertFalse(service.closed());
		
		service.close();
		
		assertTrue(service.closed());
	}
	
	@Test
	public void testHasNext_ThrowsIfClosed() {
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.hasNext());
		assertEquals("Iterator already closed", e.getMessage());
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
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}

	@SuppressWarnings("resource")
	@Test
	public void testEquals() {
		IteratorStub<?> it1 = new IteratorStub<>(data2); it1.close();
		assertTrue(service.equals(service));
		assertTrue(service.equals(new IteratorStub<>(data2)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new IteratorStub<>(data3)));
		assertFalse(service.equals(it1)); // closed
	}

}
