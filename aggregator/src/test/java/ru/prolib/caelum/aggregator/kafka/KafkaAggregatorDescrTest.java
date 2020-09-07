package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.AggregatorType;
import ru.prolib.caelum.core.Interval;

public class KafkaAggregatorDescrTest {
	KafkaAggregatorDescr service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M1, "items", "tuples-m1", "store-m1");
	}
	
	@Test
	public void testGetters() {
		assertEquals(AggregatorType.ITEM, service.getType());
		assertEquals(Interval.M1, service.getInterval());
		assertEquals("items", service.getSource());
		assertEquals("tuples-m1", service.getTarget());
		assertEquals("store-m1", service.getStoreName());
	}
	
	@Test
	public void testToString() {
		String expected = "KafkaAggregatorDescr[type=ITEM,interval=M1,source=items,target=tuples-m1,storeName=store-m1]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(540441, 709)
				.append(AggregatorType.ITEM)
				.append(Interval.M1)
				.append("items")
				.append("tuples-m1")
				.append("store-m1")
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M1, "items", "tuples-m1", "store-m1")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.TUPLE, Interval.M1, "items", "tuples-m1", "store-m1")));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.ITEM,  Interval.M5, "items", "tuples-m1", "store-m1")));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.ITEM,  Interval.M1, "zetta", "tuples-m1", "store-m1")));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.ITEM,  Interval.M1, "items", "kappa-206", "store-m1")));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.ITEM,  Interval.M1, "items", "tuples-m1", "charlie5")));
		assertFalse(service.equals(new KafkaAggregatorDescr(AggregatorType.TUPLE, Interval.M5, "zetta", "kappa-206", "charlie5")));
	}

}
