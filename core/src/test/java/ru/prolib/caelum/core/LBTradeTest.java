package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LBTradeTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	LBTrade service;

	@Before
	public void setUp() throws Exception {
		service = new LBTrade(88845122755456712L, 15, 765571L, 10);
	}
	
	@Test
	public void testCtor4I() {
		assertEquals(88845122755456712L, service.getPrice());
		assertEquals(15, service.getPriceDecimals());
		assertEquals(765571L, service.getVolume());
		assertEquals(10, service.getVolumeDecimals());
		assertNull(service.getHeader());
	}
	
	@Test
	public void testCtor4I_ThrowsIfPriceDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals expected to be in range 0-15 but: -1");
		
		new LBTrade(16625L, -1, 667L, 12);
	}
	
	@Test
	public void testCtor4I_ThrowsIfPriceDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals expected to be in range 0-15 but: 17");
		
		new LBTrade(61728L, 17, 667L, 12);
	}
	
	@Test
	public void testCtor4I_ThrowsIfVolumeDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: -3");
		
		new LBTrade(7162578L, 7, 8866661L, -3);
	}
	
	@Test
	public void testCtor4I_ThrowsIfVolumeDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: 26");
		
		new LBTrade(7326279L, 7, 8866712L, 26);
	}
	
	@Test
	public void testCtor5B() {
		service = new LBTrade(1717299987712L, (byte) 4, 71517288293L, (byte) 15, (byte) 215);

		assertEquals(1717299987712L, service.getPrice());
		assertEquals(4, service.getPriceDecimals());
		assertEquals(71517288293L, service.getVolume());
		assertEquals(15, service.getVolumeDecimals());
		assertEquals(Byte.valueOf((byte) 215), service.getHeader());
	}
	
	@Test
	public void testCtor45_HasNoRestrictionsOnDecimals() {
		service = new LBTrade(-18262673892L, (byte) -102, 8187162829L, (byte) 38, (byte) 172);
		
		assertEquals(-18262673892L, service.getPrice());
		assertEquals(-102, service.getPriceDecimals());
		assertEquals(8187162829L, service.getVolume());
		assertEquals(38, service.getVolumeDecimals());
		assertEquals(Byte.valueOf((byte)172), service.getHeader());
	}
	
	@Test
	public void testToString() {
		String expected =
			"LBTrade[price=88845122755456712,priceDecimals=15,volume=765571,volumeDecimals=10,header=<null>]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		service = new LBTrade(6671L, (byte)2, 115248L, (byte)5, (byte)130);
		int expected = new HashCodeBuilder(7182891, 45)
				.append(6671L)
				.append((byte)2)
				.append(115248L)
				.append((byte)5)
				.append(Byte.valueOf((byte)130))
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
		service = new LBTrade(88845122755456712L, (byte)15, 765571L, (byte)10, (byte)25);
		assertTrue(service.equals(new LBTrade(88845122755456712L, (byte)15, 765571L, (byte)10, (byte)25)));
		assertFalse(service.equals(new LBTrade(11111111111111111L, (byte)15, 765571L, (byte)10, (byte)25)));
		assertFalse(service.equals(new LBTrade(88845122755456712L, (byte)11, 765571L, (byte)10, (byte)25)));
		assertFalse(service.equals(new LBTrade(88845122755456712L, (byte)15, 111111L, (byte)10, (byte)25)));
		assertFalse(service.equals(new LBTrade(88845122755456712L, (byte)15, 765571L, (byte)11, (byte)25)));
		assertFalse(service.equals(new LBTrade(88845122755456712L, (byte)15, 765571L, (byte)10, (byte)11)));
		assertFalse(service.equals(new LBTrade(11111111111111111L, (byte)11, 111111L, (byte)11, (byte)11)));
	}

}
