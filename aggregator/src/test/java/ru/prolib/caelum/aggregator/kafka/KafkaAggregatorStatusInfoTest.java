package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.apache.kafka.streams.KafkaStreams.State.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class KafkaAggregatorStatusInfoTest {
	KafkaAggregatorStatusInfo service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaAggregatorStatusInfo("foo", "bar", "gap", true, NOT_RUNNING);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo", service.getSource());
		assertEquals("bar", service.getTarget());
		assertEquals("gap", service.getStore());
		assertTrue(service.getAvailability());
		assertEquals(NOT_RUNNING, service.getState());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(78910263, 301)
				.append("foo")
				.append("bar")
				.append("gap")
				.append(true)
				.append(NOT_RUNNING)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorStatusInfo("foo", "bar", "gap", true, NOT_RUNNING)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("fff", "bar", "gap", true,  NOT_RUNNING)));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("foo", "bbb", "gap", true,  NOT_RUNNING)));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("foo", "bar", "ggg", true,  NOT_RUNNING)));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("foo", "bar", "gap", false, NOT_RUNNING)));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("foo", "bar", "gap", true,  ERROR)));
		assertFalse(service.equals(new KafkaAggregatorStatusInfo("fff", "bbb", "ggg", false, ERROR)));
	}

	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaAggregatorStatusInfo[")
				.append("source=foo,")
				.append("target=bar,")
				.append("store=gap,")
				.append("availability=true,")
				.append("state=NOT_RUNNING")
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}

}
