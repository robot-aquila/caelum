package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.itemdb.kafka.Item;
import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class ItemMvcAdapterTest {
	KafkaItem item;
	Item itemData;
	ItemMvcAdapter service;

	@Before
	public void setUp() throws Exception {
		item = new KafkaItem(45000L, (byte) 2, 100000L, (byte)5, ItemType.LONG_REGULAR);
		itemData = new Item("kappa", 1282626883L, 1001L, item);
		service = new ItemMvcAdapter(itemData);
	}

	@Test
	public void testGetters() {
		assertSame(itemData, service.getItem());
		assertEquals("kappa", service.getSymbol());
		assertEquals(1282626883L, service.getTimeMillis());
		assertEquals(1001L, service.getOffset());
		assertEquals(Instant.ofEpochMilli(1282626883L).toString(), service.getTime());
		assertEquals(ItemType.LONG_REGULAR, service.getType());
		assertEquals("450.00", service.getValue());
		assertEquals("1.00000", service.getVolume());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(172076541, 703)
				.append(itemData)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ItemMvcAdapter(new Item("kappa", 1282626883L, 1001L,
				new KafkaItem(45000L, (byte) 2, 100000L, (byte)5, ItemType.LONG_REGULAR)))));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new ItemMvcAdapter(new Item("falco", 1397151240L, 1111L,
				new KafkaItem(45000L, (byte) 2, 100000L, (byte)5, ItemType.LONG_COMPACT)))));
	}

}
