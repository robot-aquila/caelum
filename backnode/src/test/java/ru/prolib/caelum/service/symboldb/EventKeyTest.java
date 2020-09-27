package ru.prolib.caelum.service.symboldb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class EventKeyTest {
	EventKey service;

	@Before
	public void setUp() throws Exception {
		service = new EventKey("foo", 100500L, 5010);
	}
	
	@Test
	public void testCtor() {
		assertEquals("foo", service.getSymbol());
		assertEquals(100500L, service.getTime());
		assertEquals(5010, service.getEventID());
	}
	
	@Test
	public void testToString() {
		String expected = "EventKey[symbol=foo,time=100500,eventID=5010]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(100235, 91)
				.append("foo")
				.append(100500L)
				.append(5010)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new EventKey("foo", 100500L, 5010)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new EventKey("aaa", 100500L, 5010)));
		assertFalse(service.equals(new EventKey("foo", 111555L, 5010)));
		assertFalse(service.equals(new EventKey("foo", 100500L, 5555)));
		assertFalse(service.equals(new EventKey("aaa", 111555L, 5555)));
	}

}
