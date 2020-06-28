package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class CategorySymbolTest {
	CategorySymbol service;

	@Before
	public void setUp() throws Exception {
		service = new CategorySymbol("foo", "bar");
	}
	
	@Test
	public void testCtor2() {
		assertEquals("foo", service.getCategory());
		assertEquals("bar", service.getSymbol());
	}
	
	@Test
	public void testToString() {
		String expected = "CategorySymbol[category=foo,symbol=bar]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(90014257, 19)
				.append("foo")
				.append("bar")
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new CategorySymbol("foo", "bar")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new CategorySymbol("aaa", "bar")));
		assertFalse(service.equals(new CategorySymbol("foo", "aaa")));
		assertFalse(service.equals(new CategorySymbol("aaa", "aaa")));
	}

}
