package ru.prolib.caelum.feeder.ak;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class KafkaItemSerdeTest {
	KafkaItemSerde service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItemSerde();
	}
	
	@Test
	public void testGetters() {
		assertEquals(new KafkaItemSerializer(), service.serializer());
		assertEquals(new KafkaItemDeserializer(), service.deserializer());
	}
	
	@Test
	public void testHashCode() {
		int expected = 30719;
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaItemSerde()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
