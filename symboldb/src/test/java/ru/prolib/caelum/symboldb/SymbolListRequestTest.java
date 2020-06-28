package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SymbolListRequestTest {
	SymbolListRequest service;

	@Before
	public void setUp() throws Exception {
		service = new SymbolListRequest("foo", "bar", 500L);
	}
	
	@Test
	public void testCtor3() {
		assertEquals("foo", service.getCategory());
		assertEquals("bar", service.getAfterSymbol());
		assertEquals(500L, service.getLimit());
	}
	
	@Test
	public void testCtor2() {
		service = new SymbolListRequest("foo", 250L);
		assertEquals("foo", service.getCategory());
		assertNull(service.getAfterSymbol());
		assertEquals(250L, service.getLimit());
	}
	
	@Test
	public void testToString() {
		String expected = "SymbolListRequest[category=foo,afterSymbol=bar,limit=500]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(221610091, 103)
				.append("foo")
				.append("bar")
				.append(500L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolListRequest("foo", "bar", 500L)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new SymbolListRequest("aaa", "bar", 500L)));
		assertFalse(service.equals(new SymbolListRequest("foo", "aaa", 500L)));
		assertFalse(service.equals(new SymbolListRequest("foo", "bar", 555L)));
		assertFalse(service.equals(new SymbolListRequest("aaa", "aaa", 555L)));
	}

}