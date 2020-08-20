package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class HostInfoTest {
	HostInfo service;

	@Before
	public void setUp() throws Exception {
		service = new HostInfo("foobar", 2345);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getHost());
		assertEquals(2345, service.getPort());
	}
	
	@Test
	public void testToString() {
		String expected = "HostInfo[host=foobar,port=2345]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1002875, 51)
				.append("foobar")
				.append(2345)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new HostInfo("foobar", 2345)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new HostInfo("barbar", 2345)));
		assertFalse(service.equals(new HostInfo("foobar", 2222)));
		assertFalse(service.equals(new HostInfo("barbar", 2222)));
	}

}
