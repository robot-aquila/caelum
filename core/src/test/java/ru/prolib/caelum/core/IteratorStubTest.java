package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IteratorStubTest {
	@Rule public ExpectedException eex = ExpectedException.none();
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
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new IteratorStub<>(data2)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new IteratorStub<>(data3)));
	}

}
