package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.ItemType;

public class ItemDataTest {
	static Item item1, item2;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		item1 = new Item(2500L, (byte)2, 100L, (byte)0, ItemType.LONG_REGULAR);
		item2 = new Item(1200L, (byte)3, 4500L, (byte)5, ItemType.LONG_REGULAR);
	}
	
	ItemData service;

	@Before
	public void setUp() throws Exception {
		service = new ItemData("foobar", 5000L, 2501L, item1);
	}
	
	@Test
	public void testCtor3() {
		assertNotNull(item1);
		assertNotNull(item2);
		
		assertEquals("foobar", service.getSymbol());
		assertEquals(5000L, service.getTime());
		assertEquals(2501L, service.getOffset());
		assertEquals(item1, service.getItem());
	}
	
	@Test
	public void testCtor1() {
		service = new ItemData(new ConsumerRecord<>("", 0, 1102L, 42600L,
				TimestampType.CREATE_TIME, 0, 0, 0, "tampa", item2));
		
		assertEquals("tampa", service.getSymbol());
		assertEquals(42600L, service.getTime());
		assertEquals(1102L, service.getOffset());
		assertEquals(item2, service.getItem());
	}
	
	@Test
	public void testGetTimeAsInstant() {
		assertEquals(Instant.ofEpochMilli(5000L), service.getTimeAsInstant());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("ItemData[symbol=foobar,time=5000,offset=2501,item=")
				.append("Item[type=LONG_REGULAR,value=2500,decimals=2,volume=100,volDecimals=0]]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(4257009, 31)
				.append("foobar")
				.append(5000L)
				.append(2501L)
				.append(item1)
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
		assertTrue(service.equals(new ItemData("foobar", 5000L, 2501L, item1)));
		assertFalse(service.equals(new ItemData("barbar", 5000L, 2501L, item1)));
		assertFalse(service.equals(new ItemData("foobar", 7000L, 2501L, item1)));
		assertFalse(service.equals(new ItemData("foobar", 5000L, 2501L, item2)));
		assertFalse(service.equals(new ItemData("foobar", 5000L, 4444L, item1)));
		assertFalse(service.equals(new ItemData("barbar", 7000L, 4444L, item2)));
	}

}
