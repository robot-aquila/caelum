package ru.prolib.caelum.core;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.ItemType.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class ItemTest {
	Item service;

	@Before
	public void setUp() throws Exception {
		service = new Item("foo", 1255839L, 5004L, 250L, (byte)2, 800L, (byte)0, LONG_COMPACT);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo", service.getSymbol());
		assertEquals(1255839L, service.getTime());
		assertEquals(5004L, service.getOffset());
		assertEquals(250L, service.getValue());
		assertEquals((byte)2, service.getDecimals());
		assertEquals(800L, service.getVolume());
		assertEquals((byte)0, service.getVolumeDecimals());
		assertEquals(LONG_COMPACT, service.getType());
	}
	
	@Test
	public void testGettemAsInstant() {
		assertEquals(Instant.ofEpochMilli(1255839L), service.getTimeAsInstant());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("Item[symbol=foo,time=1255839,offset=5004,value=250,decimals=2,")
				.append("volume=800,volDecimals=0,type=LONG_COMPACT]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(782925, 51)
				.append("foo")
				.append(1255839L)
				.append(5004L)
				.append(250L)
				.append((byte)2)
				.append(800L)
				.append((byte)0)
				.append(LONG_COMPACT)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new Item("foo", 1255839L, 5004L, 250L, (byte)2, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new Item("zzz", 1255839L, 5004L, 250L, (byte)2, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1111111L, 5004L, 250L, (byte)2, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5555L, 250L, (byte)2, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5004L, 222L, (byte)2, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5004L, 250L, (byte)0, 800L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5004L, 250L, (byte)2, 888L, (byte)0, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5004L, 250L, (byte)2, 800L, (byte)9, LONG_COMPACT)));
		assertFalse(service.equals(new Item("foo", 1255839L, 5004L, 250L, (byte)2, 800L, (byte)0, LONG_REGULAR)));
		assertFalse(service.equals(new Item("zzz", 1111111L, 5555L, 222L, (byte)0, 888L, (byte)9, LONG_REGULAR)));
	}
	
	@Test
	public void testOfDecimax15_6() {
		Item actual, expected;
		
		actual = Item.ofDecimax15("foo", 15983739L, 12000L, 3, 10L, 2);
		expected = new Item("foo", 15983739L, 0L, 12000L, (byte)3, 10L, (byte)2, LONG_COMPACT);
		assertEquals(expected, actual);
		
		actual = Item.ofDecimax15("bar", 15782923L, 250L, 2, 100L, 2);
		expected = new Item("bar", 15782923L, 0L, 250L, (byte)2, 100L, (byte)2, LONG_REGULAR);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testOfDecimax15_6_ThrowsIfDecimalsGreaterThan15() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Item.ofDecimax15("foo", 15983739L, 1200L, 18, 10L, 2));
		assertEquals("Number of decimals must be in range 0-15 but: 18", e.getMessage());
	}
	
	@Test
	public void testOfDecimax15_6_ThrowsIfVolumeDecimalsGreaterThan15() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> Item.ofDecimax15("foo", 15983739L, 1200L, 3, 10L, 44));
		assertEquals("Number of volume decimals must be in range 0-15 but: 44", e.getMessage());
	}

}
