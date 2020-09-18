package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class IntStrMapSerializerTest {
	IntStrMapSerializer service;

	@Before
	public void setUp() throws Exception {
		service = new IntStrMapSerializer();
	}

	@Test
	public void testSerialize() {
		Map<Integer, String> data = new LinkedHashMap<>();
		data.put(3, "foo");   // 1 + 1 + 1 + 3 = 6
		data.put(1001, null); // 1 + 2 = 3
		data.put(123456789, "Donec non rutrum lorem"); // 1 + 4 + 1 + 22 = 28
		data.put(-117, "negative keys should work");   // 1 + 1 + 1 + 25 = 28 
		
		byte actual[] = service.serialize(null, data);
		
		assertNotNull(actual);
		assertEquals((byte)0b00000000, actual[0]); // id len=1-1=0, dsize len=1-1=0
		assertEquals((byte)0b00000011, actual[1]); // id=3
		assertEquals((byte)0b00000011, actual[2]); // dsize=3
		assertEquals((byte)'f', actual[3]);
		assertEquals((byte)'o', actual[4]);
		assertEquals((byte)'o', actual[5]);
		
		assertEquals((byte)0b00000011, actual[6]); // id len=2-1=1, dsize=0, delete mark=1
		assertEquals((byte)0b00000011, actual[7]);
		assertEquals((byte)0b11101001, actual[8]);
		
		assertEquals((byte)0b00000110, actual[9]); // id len=4-1=3, dsize len=1-1=0
		assertEquals((byte)0b00000111, actual[10]); // id=123456789
		assertEquals((byte)0b01011011, actual[11]);
		assertEquals((byte)0b11001101, actual[12]);
		assertEquals((byte)0b00010101, actual[13]);
		assertEquals((byte)0b00010110, actual[14]); // dsize=22
		assertEquals((byte)'D', actual[15]);
		assertEquals((byte)'o', actual[16]);
		assertEquals((byte)'n', actual[17]);
		assertEquals((byte)'e', actual[18]);
		assertEquals((byte)'c', actual[19]);
		assertEquals((byte)' ', actual[20]);
		assertEquals((byte)'n', actual[21]);
		assertEquals((byte)'o', actual[22]);
		assertEquals((byte)'n', actual[23]);
		assertEquals((byte)' ', actual[24]);
		assertEquals((byte)'r', actual[25]);
		assertEquals((byte)'u', actual[26]);
		assertEquals((byte)'t', actual[27]);
		assertEquals((byte)'r', actual[28]);
		assertEquals((byte)'u', actual[29]);
		assertEquals((byte)'m', actual[30]);
		assertEquals((byte)' ', actual[31]);
		assertEquals((byte)'l', actual[32]);
		assertEquals((byte)'o', actual[33]);
		assertEquals((byte)'r', actual[34]);
		assertEquals((byte)'e', actual[35]);
		assertEquals((byte)'m', actual[36]);
		
		assertEquals((byte)0b00000000, actual[37]); // id len=1-1=0, dsize len=1-1=0
		assertEquals((byte)0b10001011, actual[38]); // id=-117
		assertEquals((byte)0b00011001, actual[39]); // dsize=25
		assertEquals((byte)'n', actual[40]);
		assertEquals((byte)'e', actual[41]);
		assertEquals((byte)'g', actual[42]);
		assertEquals((byte)'a', actual[43]);
		assertEquals((byte)'t', actual[44]);
		assertEquals((byte)'i', actual[45]);
		assertEquals((byte)'v', actual[46]);
		assertEquals((byte)'e', actual[47]);
		assertEquals((byte)' ', actual[48]);
		assertEquals((byte)'k', actual[49]);
		assertEquals((byte)'e', actual[50]);
		assertEquals((byte)'y', actual[51]);
		assertEquals((byte)'s', actual[52]);
		assertEquals((byte)' ', actual[53]);
		assertEquals((byte)'s', actual[54]);
		assertEquals((byte)'h', actual[55]);
		assertEquals((byte)'o', actual[56]);
		assertEquals((byte)'u', actual[57]);
		assertEquals((byte)'l', actual[58]);
		assertEquals((byte)'d', actual[59]);
		assertEquals((byte)' ', actual[60]);
		assertEquals((byte)'w', actual[61]);
		assertEquals((byte)'o', actual[62]);
		assertEquals((byte)'r', actual[63]);
		assertEquals((byte)'k', actual[64]);
	}
	
	@Test
	public void testHashCode() {
		assertEquals(171899105, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new IntStrMapSerializer()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
