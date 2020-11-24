package ru.prolib.caelum.lib.data;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class RawTupleTest {
	
	static Bytes toBytes(String hex) {
		return ByteUtils.hexStringToBytes(hex);
	}
	
	private Bytes open, high, low, close, volume;
	private RawTuple service;

	@Before
	public void setUp() throws Exception {
		open = toBytes("01 05 AF EE 25");
		high = toBytes("65 FF 35 00 01");
		low = toBytes("12 00");
		close = toBytes("01 FE EE");
		volume = toBytes("10 00 D0");
		service = new RawTuple(open, high, low, close, 5, volume, 10);
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
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("RawTuple[")
				.append("open={01 05 AF EE 25},")
				.append("high={65 FF 35 00 01},")
				.append("low={12 00},")
				.append("close={01 FE EE},")
				.append("decimals=5,")
				.append("volume={10 00 D0},")
				.append("volumeDecimals=10")
				.append("]")
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
		assertTrue(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				5,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("02 15 FF EE"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				5,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("02 12 AB"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				5,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("32 15 F1"),
				toBytes("01 FE EE"),
				5,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("EE 63 15"),
				5,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				8,
				toBytes("10 00 D0"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				5,
				toBytes("D0 DC 15"),
				10
			)));
		assertFalse(service.equals(new RawTuple(
				toBytes("01 05 AF EE 25"),
				toBytes("65 FF 35 00 01"),
				toBytes("12 00"),
				toBytes("01 FE EE"),
				5,
				toBytes("10 00 D0"),
				5
			)));
	}

}
