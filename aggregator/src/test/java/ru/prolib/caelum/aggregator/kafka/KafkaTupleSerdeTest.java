package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class KafkaTupleSerdeTest {
	KafkaTupleSerde service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaTupleSerde();
	}

	@Test
	public void testGetters() {
		assertEquals(new KafkaTupleSerializer(), service.serializer());
		assertEquals(new KafkaTupleDeserializer(), service.deserializer());
	}
	
	@Test
	public void testHashCode() {
		int expected = 7200172;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("resource")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaTupleSerde()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
