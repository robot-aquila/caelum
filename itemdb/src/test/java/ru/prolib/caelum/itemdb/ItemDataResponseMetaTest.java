package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ItemDataResponseMetaTest {
	ItemDataResponseMeta service;

	@Before
	public void setUp() throws Exception {
		service = new ItemDataResponseMeta(2500L, "xyz");
	}
	
	@Test
	public void testGetters() {
		assertEquals(2500L, service.getOffset());
		assertEquals("xyz", service.getMagic());
	}
	
	@Test
	public void testToString() {
		String expected = "ItemDataResponseMeta[offset=2500,magic=xyz]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(10095303, 9)
				.append(2500L)
				.append("xyz")
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
		assertTrue(service.equals(new ItemDataResponseMeta(2500L, "xyz")));
		assertFalse(service.equals(new ItemDataResponseMeta(2000L, "xyz")));
		assertFalse(service.equals(new ItemDataResponseMeta(2500L, "zzz")));
		assertFalse(service.equals(new ItemDataResponseMeta(2000L, "zzz")));
	}

}
