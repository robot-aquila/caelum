package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ItemDataRequestTest {
	ItemDataRequest service;

	@Before
	public void setUp() throws Exception {
		service = new ItemDataRequest("foobar", 5000L, 9000L, 300);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getSymbol());
		assertEquals(5000L, service.getFrom());
		assertEquals(Instant.ofEpochMilli(5000L), service.getTimeFrom());
		assertEquals(9000L, service.getTo());
		assertEquals(Instant.ofEpochMilli(9000L), service.getTimeTo());
		assertEquals(300L, service.getLimit());
	}
	
	@Test
	public void testToString() {
		String expected = "ItemDataRequest[symbol=foobar,from=5000,to=9000,limit=300]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(99766117, 93)
				.append("foobar")
				.append(5000L)
				.append(9000L)
				.append(300L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals_SpecialCases() {
		assertTrue(service.equals(service));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(new ItemDataRequest("foobar", 5000L, 9000L, 300L)));
		assertFalse(service.equals(new ItemDataRequest("barbar", 5000L, 9000L, 300L)));
		assertFalse(service.equals(new ItemDataRequest("foobar", 1000L, 9000L, 300L)));
		assertFalse(service.equals(new ItemDataRequest("foobar", 5000L, 9500L, 300L)));
		assertFalse(service.equals(new ItemDataRequest("foobar", 5000L, 9000L, 400L)));
		assertFalse(service.equals(new ItemDataRequest("barbar", 1000L, 9500L, 400L)));
	}

}
