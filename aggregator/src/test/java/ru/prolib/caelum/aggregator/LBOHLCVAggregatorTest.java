package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.OHLCVRecordType.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.LBOHLCVMutable;

public class LBOHLCVAggregatorTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	LBOHLCVAggregator service;
	LBOHLCVMutable aggregate, expected, actual;
	
	@Before
	public void setUp() throws Exception {
		service = new LBOHLCVAggregator();
		aggregate = new LBOHLCVMutable();
	}
	
	@Test
	public void testApply_FirstValue_Regular() {
		actual = service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_FirstValue_WideVol() {
		actual = service.apply("bar", new LBOHLCVMutable(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		
		expected = new LBOHLCVMutable(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_ThrowsIfPriceDecimalMismatch() {
		service.apply("bar", new LBOHLCVMutable(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Price decimals mismatch: symbol: bar, expected: 1, actual: 3");
		
		service.apply("bar", new LBOHLCVMutable(1400L, 1405L, 1000L, 1100L, (byte)3,
				1L, null, (byte)10, LONG_REGULAR), aggregate);
	}
	
	@Test
	public void testApply_NextValue_ThrowsIfVolumeDecimalMismatch() {
		service.apply("bar", new LBOHLCVMutable(1250L, 1300L, 1190L, 1200L, (byte)1,
				0L, new BigInteger("25000000"), (byte)10, LONG_WIDEVOL), aggregate);
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals mismatch: symbol: bar, expected: 10, actual: 7");
		
		service.apply("bar", new LBOHLCVMutable(1400L, 1405L, 1000L, 1100L, (byte)1,
				1L, null, (byte)7, LONG_REGULAR), aggregate);
	}
	
	@Test
	public void testApply_NextValue_NewHigh() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L, 1350L, 1200L, 1230L, (byte)2,
				500L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1350L, 1190L, 1230L, (byte)2,
				600L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertEquals(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_NewLow() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				100L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L, 1210L, 1150L, 1240L, (byte)2,
				700L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1150L, 1240L, (byte)2,
				800L, null, (byte)0, LONG_REGULAR);
		assertEquals(expected, actual);
		assertEquals(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_SwitchedToBigInt_TwoLongs() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				Long.MAX_VALUE, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L, 1210L, 1200L, 1200L, (byte)2,
				Long.MAX_VALUE, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("18446744073709551614"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_NextTrade_SwitchedToBigInt_LongInValue() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				0L, new BigInteger("788891008612"), (byte)0, LONG_WIDEVOL), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L, 1210L, 1200L, 1200L, (byte)2,
				350L, null, (byte)0, LONG_REGULAR), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("788891008962"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_NextTrade_SwitchedToBigInt_LongInAggregate() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				12000L, null, (byte)0, LONG_REGULAR), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L, 1210L, 1200L, 1200L, (byte)2,
				0L, new BigInteger("80000871200"), (byte)0, LONG_WIDEVOL), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("80000883200"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}
	
	@Test
	public void testApply_NextValue_SwitchedToBigInt_TwoBigInts() {
		service.apply("bar", new LBOHLCVMutable(1200L, 1250L, 1190L, 1205L, (byte)2,
				0L, new BigInteger("788891008612"), (byte)0, LONG_WIDEVOL), aggregate);
		
		actual = service.apply("bar", new LBOHLCVMutable(1210L,  1210L, 1200L, 1200L, (byte)2,
				0L, new BigInteger("690000126661"), (byte)0, LONG_WIDEVOL), aggregate);
		
		expected = new LBOHLCVMutable(1200L, 1250L, 1190L, 1200L, (byte)2,
				0L, new BigInteger("1478891135273"), (byte)0, LONG_WIDEVOL);
		assertEquals(expected, actual);
		assertSame(aggregate, actual);
	}

}