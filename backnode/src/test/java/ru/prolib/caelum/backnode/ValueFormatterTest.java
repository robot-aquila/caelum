package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.ValueFormatter;

public class ValueFormatterTest {
	ValueFormatter service;

	@Before
	public void setUp() throws Exception {
		service = new ValueFormatter();
	}

	@Test
	public void testFormat2() {
		assertEquals("256", service.format(256L, 0));
		assertEquals("25.6", service.format(256L, 1));
		assertEquals("2.56", service.format(256L, 2));
		assertEquals("0.256", service.format(256L,  3));
		assertEquals("0.0256", service.format(256L,  4));
		assertEquals("0.000256", service.format(256L, 6));
	}

	@Test
	public void testFormat3() {
		assertEquals("256", service.format(256L, null, 0));
		assertEquals("256", service.format(0L, new BigInteger("256"), 0));
		assertEquals("25.6", service.format(256L, null, 1));
		assertEquals("25.6", service.format(0L, new BigInteger("256"), 1));
		assertEquals("2.56", service.format(256L, null, 2));
		assertEquals("2.56", service.format(0L, new BigInteger("256"), 2));
		assertEquals("0.256", service.format(256L, null, 3));
		assertEquals("0.256", service.format(0L, new BigInteger("256"), 3));
		assertEquals("0.0256", service.format(256L, null, 4));
		assertEquals("0.0256", service.format(0L, new BigInteger("256"), 4));
		assertEquals("0.000256", service.format(256L, null, 6));
		assertEquals("0.000256", service.format(0L, new BigInteger("256"), 6));
		assertEquals("0.00110000076152", service.format(0L,  new BigInteger("110000076152"), 14));
		assertEquals("771.000771667882233", service.format(0L, new BigInteger("771000771667882233"), 15));
	}

}
