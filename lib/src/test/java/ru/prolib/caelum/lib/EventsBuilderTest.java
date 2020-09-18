package ru.prolib.caelum.lib;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class EventsBuilderTest {
	EventsBuilder service;

	@Before
	public void setUp() throws Exception {
		service = new EventsBuilder();
	}
	
	@Test
	public void testBuild_ShouldThrowIfSymbolWasNotDefined() {
		service.withTime(5678912345L).withEvent(1001, "hello");
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.build());
		
		assertEquals("Symbol was not defined", e.getMessage());
	}
	
	@Test
	public void testBuild_ShouldThrowIfTimeWasNotDefined() {
		service.withSymbol("foo@bar").withEvent(1001, "hello");
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.build());
		
		assertEquals("Time was not defined", e.getMessage());
	}
	
	@Test
	public void testBuild_ShouldThrowIfNoEventsDefined() {
		service.withSymbol("foo@bar").withTime(123456789L);
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.build());
		
		assertEquals("No events defined", e.getMessage());
	}

	@Test
	public void testBuild() {
		assertSame(service, service.withSymbol("foo@bar"));
		assertSame(service, service.withTime(123456789L));
		assertSame(service, service.withEvent(1001, "hello"));
		assertSame(service, service.withEvent(1002, "world"));
		
		Events actual = service.build();
		
		assertNotNull(actual);
		assertEquals("foo@bar", actual.getSymbol());
		assertEquals(123456789L, actual.getTime());
		assertEquals("hello", actual.getEvent(1001));
		assertEquals("world", actual.getEvent(1002));
	}
	
	@Test
	public void testBuild_ShouldMakeACopyOfEvents() {
		Events actual = service.withSymbol("pop@gap")
			.withTime(6654412228L)
			.withEvent(5001, "foo")
			.withEvent(5002, "bar")
			.build();
		service.withEvent(5001, "tutumbr")
			.withEvent(5002, "gambrinus");
		
		assertNotEquals("tutumbr", actual.getEvent(5001));
		assertNotEquals("gambrinus", actual.getEvent(5002));
	}
	
	@Test
	public void testHasEvents() {
		assertFalse(service.hasEvents());
		
		service.withEvent(1001, "gap");
		assertTrue(service.hasEvents());
	}

}
