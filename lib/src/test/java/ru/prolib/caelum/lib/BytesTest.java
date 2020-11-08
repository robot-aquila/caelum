package ru.prolib.caelum.lib;

import static org.junit.Assert.*;

import java.util.Random;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class BytesTest {
	private byte[] template = { (byte)0xF0, (byte)0x1A, (byte)0x23, (byte)0xFF, (byte)0x06, (byte)0xAB };
	private byte[] source1, source2, source3;
	private Bytes service;
	
	@Before
	public void setUp() {
		Random rnd = new Random();
		rnd.nextBytes(source1 = new byte[24]);
		rnd.nextBytes(source2 = new byte[64]);
		rnd.nextBytes(source3 = new byte[24]);
		System.arraycopy(template, 0, source1,  4, template.length);
		System.arraycopy(template, 0, source2, 24, template.length);
		service = new Bytes(source1, 4, template.length);
	}
	
	@Test
	public void testGetters() {
		assertSame(source1, service.getSource());
		assertEquals(4, service.getOffset());
		assertEquals(6, service.getLength());
	}
	
	@Test
	public void testToString() {
		String expected = "Bytes[source=" + source1 + " offset=4 length=6 hex={F0 1A 23 FF 06 AB}]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testCopyBytes1() {
		byte[] target = new byte[6];
		
		assertSame(target, service.copyBytes(target));
		
		assertArrayEquals(target, template);
	}
	
	@Test
	public void testCopyBytes0() {
		byte[] actual = service.copyBytes();
		
		assertArrayEquals(actual, template);
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(70091507, 73)
				.append(template)
				.build();
		
		assertEquals(expected, service.hashCode());
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
		assertTrue(service.equals(new Bytes(source1,  4, 6)));
		assertTrue(service.equals(new Bytes(source2, 24, 6)));
		assertFalse(service.equals(new Bytes(source1, 5, 8)));
		assertFalse(service.equals(new Bytes(source2, 4, 6)));
		assertFalse(service.equals(new Bytes(source3, 5, 2)));
		assertFalse(service.equals(new Bytes(source1, 4, 5)));
		assertFalse(service.equals(new Bytes(source1, 4, 7)));
	}

}
