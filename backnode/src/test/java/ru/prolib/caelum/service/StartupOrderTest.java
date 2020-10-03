package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static ru.prolib.caelum.service.StartupPriority.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class StartupOrderTest {
	StartupOrder service;

	@Before
	public void setUp() throws Exception {
		service = new StartupOrder(NORMAL, 15);
	}
	
	@Test
	public void testGetters() {
		assertEquals(NORMAL, service.getPriority());
		assertEquals(Integer.valueOf(15), service.getOrder());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(888164015, 3709)
				.append(NORMAL)
				.append(Integer.valueOf(15))
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new StartupOrder(NORMAL, 15)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new StartupOrder(FIRST, 15)));
		assertFalse(service.equals(new StartupOrder(NORMAL, 22)));
		assertFalse(service.equals(new StartupOrder(FIRST, 22)));
	}

}
