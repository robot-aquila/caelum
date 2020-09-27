package ru.prolib.caelum.lib;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class BitsSetIfUnsetTest {
	BitsSetIfUnset service;

	@Before
	public void setUp() throws Exception {
		service = new BitsSetIfUnset(4);
	}
	
	@Test
	public void testCtor() {
		assertFalse(service.applied());
	}
	
	@Test
	public void testApplyAsInt() {
		assertEquals(1, service.applyAsInt(0, 1));
		assertTrue(service.applied());
		
		assertEquals(3, service.applyAsInt(1, 2));
		assertTrue(service.applied());
		
		assertEquals(7, service.applyAsInt(3, 4));
		assertTrue(service.applied());
		
		assertEquals(6, service.applyAsInt(6, 8));
		assertFalse(service.applied());
		
		assertEquals(9, service.applyAsInt(1, 8));
		assertTrue(service.applied());
	}
	
	@Test
	public void testToString() {
		assertEquals("BitsSetIfUnset[cond=4,applied=false]", service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115009, 901)
				.append(4)
				.append(false)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		BitsSetIfUnset o = new BitsSetIfUnset(4);
		o.applyAsInt(1, 8);
		
		assertTrue(service.equals(service));
		assertTrue(service.equals(new BitsSetIfUnset(4)));
		assertFalse(service.equals(o));
		assertFalse(service.equals(new BitsSetIfUnset(1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
