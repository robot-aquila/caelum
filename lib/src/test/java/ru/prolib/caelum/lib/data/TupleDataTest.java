package ru.prolib.caelum.lib.data;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class TupleDataTest {
	
	static Bytes toBytes(String hex) {
		return ByteUtils.hexStringToBytes(hex);
	}
	
	private BigInteger open, high, low, close, volume;
	private TupleData service;

	@Before
	public void setUp() throws Exception {
		open = BigInteger.valueOf(117726L);
		high = BigInteger.valueOf(120096L);
		low = BigInteger.valueOf(90580L);
		close = BigInteger.valueOf(115210L);
		volume = BigInteger.valueOf(10000L);
		service = new TupleData(open, high, low, close, 5, volume, 10);
	}
	
	@Test
	public void testGetters() {
		assertSame(open, service.open());
		assertSame(high, service.high());
		assertSame(low, service.low());
		assertSame(close, service.close());
		assertSame(volume, service.volume());
		assertEquals(5, service.decimals());
		assertEquals(10, service.volumeDecimals());
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
        assertTrue(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(111111L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(222222L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(333333L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(444444L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                6,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(777777L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                11
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(111111L),
                BigInteger.valueOf(222222L),
                BigInteger.valueOf(333333L),
                BigInteger.valueOf(444444L),
                6,
                BigInteger.valueOf(777777L),
                11
            )));
	}

}
