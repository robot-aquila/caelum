package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SymbolListRequestTest {
	SymbolListRequest service;

	@Before
	public void setUp() throws Exception {
		service = new SymbolListRequest("foo", "bar", 500);
	}
	
	@Test
	public void testCtor3() {
		assertEquals("foo", service.getCategory());
		assertEquals("bar", service.getAfterSymbol());
		assertEquals(Integer.valueOf(500), service.getLimit());
	}
	
	@Test
	public void testCtor3_LimitCanBeNull() {
		service = new SymbolListRequest("foo", "foo@bar", null);
		assertEquals("foo", service.getCategory());
		assertEquals("foo@bar", service.getAfterSymbol());
		assertNull(service.getLimit());
	}
	
	@Test
	public void testCtor2() {
		service = new SymbolListRequest("foo", 250);
		assertEquals("foo", service.getCategory());
		assertNull(service.getAfterSymbol());
		assertEquals(Integer.valueOf(250), service.getLimit());
	}
	
	@Test
	public void testCtor2_LimitCanBeNull() {
		service = new SymbolListRequest("foo", null);
		assertEquals("foo", service.getCategory());
		assertNull(service.getAfterSymbol());
		assertNull(service.getLimit());
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
				.append(500)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolListRequest("foo", "bar", 500)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new SymbolListRequest("aaa", "bar", 500)));
		assertFalse(service.equals(new SymbolListRequest("foo", "aaa", 500)));
		assertFalse(service.equals(new SymbolListRequest("foo", "bar", 555)));
		assertFalse(service.equals(new SymbolListRequest("aaa", "aaa", 555)));
	}
	
}
