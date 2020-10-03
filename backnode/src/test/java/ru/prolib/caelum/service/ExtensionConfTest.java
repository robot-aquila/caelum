package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static ru.prolib.caelum.service.StartupPriority.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ExtensionConfTest {
	StartupOrder order1, order2, order3, order4;
	ExtensionConf service;

	@Before
	public void setUp() throws Exception {
		order1 = new StartupOrder(NORMAL, 15);
		order2 = new StartupOrder(FIRST, null);
		order3 = new StartupOrder(LAST, null);
		order4 = new StartupOrder(NORMAL, null);
		service = new ExtensionConf("foo", "bar", true, order1);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo", service.getId());
		assertEquals("bar", service.getBuilderClass());
		assertTrue(service.isEnabled());
		assertEquals(new StartupOrder(NORMAL, 15), service.getStartupOrder());
	}
	
	@Test
	public void testToString() {
		assertEquals("ExtensionConf[id=foo,builder=bar,enabled=true,order=15]",
				new ExtensionConf("foo", "bar", true, order1).toString());
		assertEquals("ExtensionConf[id=bar,builder=pop,enabled=false,order=first]",
				new ExtensionConf("bar", "pop", false, order2).toString());
		assertEquals("ExtensionConf[id=gap,builder=dup,enabled=true,order=last]",
				new ExtensionConf("gap", "dup", true, order3).toString());
		assertEquals("ExtensionConf[id=lol,builder=pol,enabled=false,order=normal]",
				new ExtensionConf("lol", "pol", false, order4).toString());		
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115297, 93)
				.append("foo")
				.append("bar")
				.append(true)
				.append(order1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExtensionConf("foo", "bar", true, order1)));
		assertFalse(service.equals(this));
		assertFalse(service.equals(null));
		assertFalse(service.equals(new ExtensionConf("fff", "bar", true, order1)));
		assertFalse(service.equals(new ExtensionConf("foo", "bbb", true, order1)));
		assertFalse(service.equals(new ExtensionConf("foo", "bar", false, order1)));
		assertFalse(service.equals(new ExtensionConf("foo", "bar", true, order2)));
		assertFalse(service.equals(new ExtensionConf("fff", "bbb", false, order2)));
	}

}
