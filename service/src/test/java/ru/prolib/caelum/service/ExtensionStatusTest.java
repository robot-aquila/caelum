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
		service = new ExtensionStatus("foobar", PENDING, statusInfo1);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getId());
		assertEquals(PENDING, service.getState());
		assertEquals(statusInfo1, service.getStatusInfo());
	}
	
	@Test
	public void testToString() {
		String expected = "ExtensionStatus[id=foobar,state=PENDING,statusInfo=zulu]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(414219, 901)
				.append("foobar")
				.append(PENDING)
				.append(statusInfo1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExtensionStatus("foobar", PENDING, statusInfo1)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new ExtensionStatus("barbar", PENDING, statusInfo1)));
		assertFalse(service.equals(new ExtensionStatus("foobar", CREATED, statusInfo1)));
		assertFalse(service.equals(new ExtensionStatus("foobar", PENDING, statusInfo2)));
		assertFalse(service.equals(new ExtensionStatus("barbar", CREATED, statusInfo2)));
	}

}
