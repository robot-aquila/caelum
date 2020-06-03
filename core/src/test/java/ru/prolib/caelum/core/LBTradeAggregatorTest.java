package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LBTradeAggregatorTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	LBTradeAggregator service;
	LBOHLCVMutable candle, expected, actual;

	@Before
	public void setUp() throws Exception {
		service = new LBTradeAggregator();
		candle = new LBOHLCVMutable();
	}
	
	@Test
	public void testApply_FirstTrade() {
		actual = service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		
		expected = new LBOHLCVMutable(45000L, 45000L, 45000L, 45000L, (byte)2,
				10L, null, (byte)0, OHLCVRecordType.LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(candle, actual);
	}
	
	@Test
	public void testApply_NextTrade_ThrowsIfPriceDecimalMismatch() {
		service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals mismatch: symbol: foo, expected: 2, actual: 5");
		
		service.apply("foo", new LBTrade(49000L, 5, 10, 0), candle);
	}
	
	@Test
	public void testApply_NextTrade_ThrowsIfVolumeDecimalMismatch() {
		service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals mismatch: symbol: foo, expected: 0, actual: 2");
		
		service.apply("foo", new LBTrade(49000L, 2, 10, 2), candle);
	}
	
	@Test
	public void testApply_NextTrade_NewHigh() {
		service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		
		actual = service.apply("foo", new LBTrade(49000L, 2, 1, 0), candle);
		
		expected = new LBOHLCVMutable(45000L, 49000L, 45000L, 49000L, (byte)2,
				11L, null, (byte)0, OHLCVRecordType.LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(candle, actual);
	}
	
	@Test
	public void testApply_NextTrade_NewLow() {
		service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		service.apply("foo", new LBTrade(49000L, 2, 1, 0), candle);
		
		actual = service.apply("foo", new LBTrade(43900L, 2, 5, 0), candle);
		
		expected = new LBOHLCVMutable(45000L, 49000L, 43900L, 43900L, (byte)2,
				16L, null, (byte)0, OHLCVRecordType.LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(candle, actual);
	}
	
	@Test
	public void testApply_NextTrade_SwitchedToBigInt() {
		service.apply("foo", new LBTrade(45000L, 2, 10, 0), candle);
		service.apply("foo", new LBTrade(49000L, 2,  1, 0), candle);
		service.apply("foo", new LBTrade(43900L, 2,  5, 0), candle);

		actual = service.apply("foo", new LBTrade(46000L, 2, Long.MAX_VALUE, 0), candle);
		
		expected = new LBOHLCVMutable(45000L, 49000L, 43900L, 46000L, (byte)2,
				0L, new BigInteger("9223372036854775823"), (byte)0, OHLCVRecordType.LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(candle, actual);
	}

}
