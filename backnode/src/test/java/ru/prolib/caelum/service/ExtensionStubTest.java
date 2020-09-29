package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ExtensionStubTest {
	ExtensionStatus status1, status2;
	ExtensionStub service;

	@Before
	public void setUp() throws Exception {
		status1 = new ExtensionStatus("foo", ExtensionState.PENDING, null);
		status2 = new ExtensionStatus("bar", ExtensionState.DEAD, null);
		service = new ExtensionStub(status1);
	}
	
	@Test
	public void testGetStatus( ) {
		assertEquals(status1, service.getStatus());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExtensionStub(status1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new ExtensionStub(status2)));
	}

}
