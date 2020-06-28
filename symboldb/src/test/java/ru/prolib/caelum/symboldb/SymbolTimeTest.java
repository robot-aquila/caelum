package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SymbolTimeTest {
	SymbolTime service;

	@Before
	public void setUp() throws Exception {
		service = new SymbolTime("foo", 100500L);
	}
	
	@Test
	public void testCtor() {
		assertEquals("foo", service.getSymbol());
		assertEquals(100500L, service.getTime());
	}
	
	@Test
	public void testToString() {
		String expected = "SymbolTime[symbol=foo,time=100500]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(100235, 91)
				.append("foo")
				.append(100500L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolTime("foo", 100500L)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new SymbolTime("aaa", 100500L)));
		assertFalse(service.equals(new SymbolTime("foo", 111555L)));
		assertFalse(service.equals(new SymbolTime("aaa", 111555L)));
	}

}
