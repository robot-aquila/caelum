package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ItemDataRequestContinueTest {
	ItemDataRequestContinue service;

	@Before
	public void setUp() throws Exception {
		service = new ItemDataRequestContinue("foobar", 2500L, "xyz", 19998778762L, 300);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getSymbol());
		assertEquals(2500L, service.getOffset());
		assertEquals("xyz", service.getMagic());
		assertEquals(Long.valueOf(19998778762L), service.getTo());
		assertEquals(Instant.ofEpochMilli(19998778762L), service.getTimeTo());
		assertEquals(Integer.valueOf(300), service.getLimit());
	}
	
	@Test
	public void testCtor5_NullValuesAllowedForAllParamsExceptSymbolAndOffset() {
		service = new ItemDataRequestContinue("foobar", 2500L, null, null, null);
		assertEquals("foobar", service.getSymbol());
		assertEquals(2500L, service.getOffset());
		assertNull(service.getMagic());
		assertNull(service.getTo());
		assertNull(service.getTimeTo());
		assertNull(service.getLimit());
	}
	
	@Test
	public void testToString() {
		String expected = "ItemDataRequestContinue[symbol=foobar,offset=2500,magic=xyz,to=19998778762,limit=300]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(10009827, 15)
				.append("foobar")
				.append(2500L)
				.append("xyz")
				.append(Long.valueOf(19998778762L))
				.append(Integer.valueOf(300))
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
		assertTrue(service.equals(new ItemDataRequestContinue("foobar", 2500L, "xyz", 19998778762L, 300)));
		assertFalse(service.equals(new ItemDataRequestContinue("barbar", 2500L, "xyz", 19998778762L, 300)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2000L, "xyz", 19998778762L, 300)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2500L, "aaa", 19998778762L, 300)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2500L, "xyz", 11111111111L, 300)));
		assertFalse(service.equals(new ItemDataRequestContinue("foobar", 2500L, "xyz", 19998778762L, 200)));
		assertFalse(service.equals(new ItemDataRequestContinue("barbar", 2000L, "aaa", 11111111111L, 200)));
	}

}
