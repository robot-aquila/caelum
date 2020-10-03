package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static ru.prolib.caelum.service.ExtensionState.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ExtensionStatusTest {
	ExtensionStatus service;
	Object statusInfo1, statusInfo2;

	@Before
	public void setUp() throws Exception {
		statusInfo1 = new String("zulu");
		statusInfo2 = new String("charlie");
		service = new ExtensionStatus(PENDING, statusInfo1);
	}
	
	@Test
	public void testGetters() {
		assertEquals(PENDING, service.getState());
		assertEquals(statusInfo1, service.getStatusInfo());
	}
	
	@Test
	public void testToString() {
		String expected = "ExtensionStatus[state=PENDING,statusInfo=zulu]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(414219, 901)
				.append(PENDING)
				.append(statusInfo1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExtensionStatus(PENDING, statusInfo1)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new ExtensionStatus(CREATED, statusInfo1)));
		assertFalse(service.equals(new ExtensionStatus(PENDING, statusInfo2)));
		assertFalse(service.equals(new ExtensionStatus(CREATED, statusInfo2)));
	}

}
