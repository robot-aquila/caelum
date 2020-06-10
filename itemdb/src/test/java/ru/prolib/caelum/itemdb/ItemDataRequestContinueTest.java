package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ItemDataRequestContinueTest {
	ItemDataRequestContinue service;

	@Before
	public void setUp() throws Exception {
		service = new ItemDataRequestContinue("foobar", 2500L, "xyz", 300L);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getSymbol());
		assertEquals(2500L, service.getOffset());
		assertEquals("xyz", service.getMagic());
		assertEquals(300L, service.getLimit());
	}
	
	@Test
	public void testToString() {
		String expected = "ItemDataRequestContinue[symbol=foobar,offset=2500,magic=xyz,limit=300]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(10009827, 15)
				.append("foobar")
				.append(2500L)
				.append("xyz")
				.append(300L)
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
		assertTrue(service.equals(new ItemDataRequestContinue("foobar", 2500L, "xyz", 300L)));
		assertFalse(service.equals(new ItemDataRequestContinue("barbar", 2500L, "xyz", 300L)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2000L, "xyz", 300L)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2500L, "aaa", 300L)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2500L, "xyz", 200L)));
		assertFalse(service.equals(new ItemDataRequestContinue("barbar", 2000L, "aaa", 200L)));
	}

}
