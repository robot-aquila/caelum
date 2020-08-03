package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ExtensionConfTest {
	ExtensionConf service;

	@Before
	public void setUp() throws Exception {
		service = new ExtensionConf("foo", "bar", true);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo", service.getId());
		assertEquals("bar", service.getBuilderClass());
		assertTrue(service.isEnabled());
	}
	
	@Test
	public void testToString() {
		String expected = "ExtensionConf[id=foo,builder=bar,enabled=true]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115297, 93)
				.append("foo")
				.append("bar")
				.append(true)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExtensionConf("foo", "bar", true)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new ExtensionConf("fff", "bar", true)));
		assertFalse(service.equals(new ExtensionConf("foo", "bbb", true)));
		assertFalse(service.equals(new ExtensionConf("foo", "bar", false)));
		assertFalse(service.equals(new ExtensionConf("fff", "bbb", false)));
	}

}
