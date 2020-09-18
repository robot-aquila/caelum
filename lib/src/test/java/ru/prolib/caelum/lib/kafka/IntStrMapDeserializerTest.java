package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class IntStrMapDeserializerTest {
	IntStrMapDeserializer service;

	@Before
	public void setUp() throws Exception {
		service = new IntStrMapDeserializer();
	}
	
	@Test
	public void testDeserialize() {
		byte data[] = {
			0b00000000, 0b00000011, 0b00000011, 'f', 'o', 'o',
			
			0b00000011, 0b00000011, (byte)0b11101001, 
			
			0b00000110, 0b00000111, 0b01011011, (byte)0b11001101, 0b00010101, 0b00010110,
			'D','o','n','e','c',' ','n','o','n',' ','r','u','t','r','u','m',' ','l','o','r','e','m',
			
			0b00000000, (byte)0b10001011, 0b00011001,
			'n','e','g','a','t','i','v','e',' ','k','e','y','s',' ','s','h','o','u','l','d',' ','w','o','r','k',
		};
		
		Map<Integer, String> actual = service.deserialize(null, data);
		
		Map<Integer, String> expected = new HashMap<>();
		expected.put(3, "foo");
		expected.put(1001, null);
		expected.put(123456789, "Donec non rutrum lorem");
		expected.put(-117, "negative keys should work"); 
		assertEquals(expected, actual);
	}
	
	@Test
	public void testHashCode() {
		assertEquals(2009865103, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new IntStrMapDeserializer()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
