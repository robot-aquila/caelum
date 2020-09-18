package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class EventListRequestTest {
	EventListRequest service;

	@Before
	public void setUp() throws Exception {
		service = new EventListRequest("foo@bar", 12227736L, 13998720L, 100);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo@bar", service.getSymbol());
		assertEquals(Long.valueOf(12227736L), service.getFrom());
		assertEquals(Instant.ofEpochMilli(12227736L), service.getTimeFrom());
		assertEquals(Long.valueOf(13998720L), service.getTo());
		assertEquals(Instant.ofEpochMilli(13998720L), service.getTimeTo());
		assertEquals(Integer.valueOf(100), service.getLimit());
	}
	
	@Test
	public void testCtor1() {
		service = new EventListRequest("pop@gap");
		assertEquals("pop@gap", service.getSymbol());
		assertNull(service.getFrom());
		assertNull(service.getTo());
		assertNull(service.getLimit());
	}
	
	@Test
	public void testGetTimeFrom_ShouldReturnNullIfNotDefined() {
		service = new EventListRequest("foo@bar", null, 13998720L, 100);
		
		assertNull(service.getTimeFrom());
	}
	
	@Test
	public void testGetTimeTo_ShouldReturnNullIfNotDefined() {
		service = new EventListRequest("foo@bar", 12227736L, null, 100);
		
		assertNull(service.getTimeTo());
	}
	
	@Test
	public void testIsValid_ShouldReturnFalseIfSymbolNotDefined() {
		service = new EventListRequest(null, 12227736L, 13998720L, 100);
		
		assertFalse(service.isValid());
	}
	
	@Test
	public void testIsValid_ShouldReturnFalseIfSymbolLengthZero() {
		service = new EventListRequest("", 12227736L, 13998720L, 100);
		
		assertFalse(service.isValid());
	}
	
	@Test
	public void testToString() {
		String expected = "EventListRequest[symbol=foo@bar,from=12227736,to=13998720,limit=100]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(5370241, 47)
				.append("foo@bar")
				.append(12227736L)
				.append(13998720L)
				.append(100)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new EventListRequest("foo@bar", 12227736L, 13998720L, 100)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new EventListRequest("foo@foo", 12227736L, 13998720L, 100)));
		assertFalse(service.equals(new EventListRequest("foo@bar", 11111111L, 13998720L, 100)));
		assertFalse(service.equals(new EventListRequest("foo@bar", 12227736L, 22222222L, 100)));
		assertFalse(service.equals(new EventListRequest("foo@bar", 12227736L, 13998720L, 111)));
		assertFalse(service.equals(new EventListRequest("foo@foo", 11111111L, 22222222L, 111)));
	}

}
