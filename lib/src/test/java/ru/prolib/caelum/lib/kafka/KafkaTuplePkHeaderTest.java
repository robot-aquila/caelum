package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class KafkaTuplePkHeaderTest {
	private KafkaTuplePkHeader service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 3, true, 2, 4);
	}
	
	@Test
	public void testGetters() {
		assertEquals(5, service.decimals());
		assertEquals(3, service.volumeDecimals());
		assertEquals(8, service.openSize());
		assertTrue(service.isHighRelative());
		assertEquals(7, service.highSize());
		assertFalse(service.isLowRelative());
		assertEquals(3, service.lowSize());
		assertTrue(service.isCloseRelative());
		assertEquals(2, service.closeSize());
		assertEquals(4, service.volumeSize());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaTuplePkHeader[decimals=5, volumeDecimals=3, openSize=8, ")
				.append("isHighRelative=true, highSize=7, isLowRelative=false, lowSize=3, ")
				.append("isCloseRelative=true, closeSize=2, volumeSize=4]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals_SpecialCases() {
		assertTrue(service.equals(service));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(6, 3, 8, true, 7, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 4, 8, true, 7, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 9, true, 7, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, false, 7, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 8, false, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, true, 3, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 4, true, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 3, false, 2, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 3, true, 3, 4)));
		assertFalse(service.equals(new KafkaTuplePkHeader(5, 3, 8, true, 7, false, 3, true, 2, 5)));
	}

}
