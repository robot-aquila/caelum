package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class KafkaItemTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
	KafkaItem service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItem(88845122755456712L, 15, 765571L, 10);
	}
	
	@Test
	public void testCtor4() {
		assertEquals(88845122755456712L, service.getValue());
		assertEquals(15, service.getDecimals());
		assertEquals(765571L, service.getVolume());
		assertEquals(10, service.getVolumeDecimals());
		assertEquals(ItemType.LONG_UNKNOWN, service.getType());
	}
	
	@Test
	public void testCtor4_ThrowsIfValueDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Value decimals expected to be in range 0-15 but: -1");
		
		new KafkaItem(16625L, -1, 667L, 12);
	}
	
	@Test
	public void testCtor4_ThrowsIfValueDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Value decimals expected to be in range 0-15 but: 17");
		
		new KafkaItem(61728L, 17, 667L, 12);
	}
	
	@Test
	public void testCtor4_ThrowsIfVolumeDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: -3");
		
		new KafkaItem(7162578L, 7, 8866661L, -3);
	}
	
	@Test
	public void testCtor4_ThrowsIfVolumeDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: 26");
		
		new KafkaItem(7326279L, 7, 8866712L, 26);
	}
	
	@Test
	public void testCtor5() {
		service = new KafkaItem(1717299987712L, (byte) 4, 71517288293L, (byte) 15, ItemType.LONG_REGULAR);

		assertEquals(1717299987712L, service.getValue());
		assertEquals(4, service.getDecimals());
		assertEquals(71517288293L, service.getVolume());
		assertEquals(15, service.getVolumeDecimals());
		assertEquals(ItemType.LONG_REGULAR, service.getType());
	}
	
	@Test
	public void testCtor5_ThrowsIfValueDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Value decimals expected to be in range 0-15 but: -1");
		
		new KafkaItem(16625L, (byte)-1, 667L, (byte)12, ItemType.LONG_UNKNOWN);
	}
	
	@Test
	public void testCtor5_ThrowsIfValueDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Value decimals expected to be in range 0-15 but: 17");
		
		new KafkaItem(61728L, (byte)17, 667L, (byte)12, ItemType.LONG_UNKNOWN);
	}
	
	@Test
	public void testCtor5_ThrowsIfVolumeDecimalsLtZero() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: -3");
		
		new KafkaItem(7162578L, (byte)7, 8866661L, (byte)-3, ItemType.LONG_UNKNOWN);
	}
	
	@Test
	public void testCtor5_ThrowsIfVolumeDecimalsGt15() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Volume decimals expected to be in range 0-15 but: 26");
		
		new KafkaItem(7326279L, (byte)7, 8866712L, (byte)26, ItemType.LONG_UNKNOWN);
	}
	
	@Test
	public void testToString() {
		String expected =
			"KafkaItem[type=LONG_UNKNOWN,value=88845122755456712,decimals=15,volume=765571,volDecimals=10]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		service = new KafkaItem(6671L, (byte)2, 115248L, (byte)5, ItemType.LONG_REGULAR);
		int expected = new HashCodeBuilder(7182891, 45)
				.append(6671L)
				.append((byte)2)
				.append(115248L)
				.append((byte)5)
				.append(ItemType.LONG_REGULAR)
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
		service = new KafkaItem(88845122755456712L, (byte)15, 765571L, (byte)10, ItemType.LONG_REGULAR);
		assertTrue(service.equals(new KafkaItem(88845122755456712L, (byte)15, 765571L, (byte)10, ItemType.LONG_REGULAR)));
		assertFalse(service.equals(new KafkaItem(11111111111111111L, (byte)15, 765571L, (byte)10, ItemType.LONG_REGULAR)));
		assertFalse(service.equals(new KafkaItem(88845122755456712L, (byte)11, 765571L, (byte)10, ItemType.LONG_REGULAR)));
		assertFalse(service.equals(new KafkaItem(88845122755456712L, (byte)15, 111111L, (byte)10, ItemType.LONG_REGULAR)));
		assertFalse(service.equals(new KafkaItem(88845122755456712L, (byte)15, 765571L, (byte)11, ItemType.LONG_REGULAR)));
		assertFalse(service.equals(new KafkaItem(88845122755456712L, (byte)15, 765571L, (byte)10, ItemType.LONG_COMPACT)));
		assertFalse(service.equals(new KafkaItem(11111111111111111L, (byte)11, 111111L, (byte)11, ItemType.LONG_COMPACT)));
	}

}
