package ru.prolib.caelum.lib;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class EventsTest {
	Map<Integer, String> events;
	Events service;

	@Before
	public void setUp() throws Exception {
		events = new LinkedHashMap<>();
		events.put(1001, "hello");
		events.put(1002, "world");
		service = new Events("foo@bar", 123456789L, events);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo@bar", service.getSymbol());
		assertEquals(123456789L, service.getTime());
		Set<Integer> expected_event_ids = new LinkedHashSet<>();
		expected_event_ids.add(1001);
		expected_event_ids.add(1002);
		assertEquals(expected_event_ids, service.getEventIDs());
		assertTrue(service.hasEvent(1001));
		assertEquals("hello", service.getEvent(1001));
		assertTrue(service.hasEvent(1002));
		assertEquals("world", service.getEvent(1002));
	}
	
	@Test
	public void testIsEventDelete() {
		events.put(1001, null);
		events.put(1002, "foobar");
		
		assertTrue(service.isEventDelete(1001));
		assertFalse(service.isEventDelete(1002));
		assertFalse(service.isEventDelete(1003));
	}
	
	@Test
	public void testToString() {
		String expected = "Events[symbol=foo@bar,time=123456789,events={1001=hello, 1002=world}]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(7760129, 903)
				.append("foo@bar")
				.append(123456789L)
				.append(events)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		Map<Integer, String> events1 = new LinkedHashMap<>();
		events1.put(1001, "hello");
		events1.put(1002, "world");
		Map<Integer, String> events2 = new LinkedHashMap<>();
		events2.put(1001, "hello");
		events2.put(1005, "bumba");
		assertTrue(service.equals(service));
		assertTrue(service.equals(new Events("foo@bar", 123456789L, events1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new Events("bar@buz", 123456789L, events1)));
		assertFalse(service.equals(new Events("foo@bar", 111111111L, events1)));
		assertFalse(service.equals(new Events("foo@bar", 123456789L, events2)));
		assertFalse(service.equals(new Events("bar@buz", 111111111L, events2)));
	}

}
