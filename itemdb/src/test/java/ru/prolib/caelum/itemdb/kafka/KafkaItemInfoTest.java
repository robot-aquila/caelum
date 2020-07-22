package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class KafkaItemInfoTest {
	KafkaItemInfo service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItemInfo("foobar", 5, "zulu", 2, 500L, 2500L);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getTopic());
		assertEquals(5, service.getNumPartitions());
		assertEquals("zulu", service.getSymbol());
		assertEquals(2, service.getPartition());
		assertEquals(Long.valueOf(500L), service.getStartOffset());
		assertEquals(Long.valueOf(2500L), service.getEndOffset());
	}
	
	@Test
	public void testToTopicPartition() {
		assertEquals(new TopicPartition("foobar", 2), service.toTopicPartition());
	}
	
	@Test
	public void testHasData() {
		assertTrue(new KafkaItemInfo("xxx", 5, "zzz", 2, 100L, 200L).hasData());
		assertFalse(new KafkaItemInfo("xxx", 5, "zzz", 2, null, 200L).hasData());
		assertFalse(new KafkaItemInfo("xxx", 5, "zzz", 2, 100L, null).hasData());
		assertFalse(new KafkaItemInfo("xxx", 5, "zzz", 2, null, null).hasData());
		assertFalse(new KafkaItemInfo("xxx", 5, "zzz", 2, 100L, 100L).hasData());
		assertFalse(new KafkaItemInfo("xxx", 5, "zzz", 2, 200L, 100L).hasData());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaItemInfo[")
				.append("topic=foobar,")
				.append("numPartitions=5,")
				.append("symbol=zulu,")
				.append("partition=2,")
				.append("startOffset=500,")
				.append("endOffset=2500")
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1403597, 11)
				.append("foobar")
				.append(5)
				.append("zulu")
				.append(2)
				.append(Long.valueOf(500L))
				.append(Long.valueOf(2500L))
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
		assertTrue(service.equals(new KafkaItemInfo("foobar", 5, "zulu", 2, 500L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("barbar", 5, "zulu", 2, 500L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("foobar", 8, "zulu", 2, 500L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("foobar", 5, "gamu", 2, 500L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("foobar", 5, "zulu", 3, 500L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("foobar", 5, "zulu", 2, 800L, 2500L)));
		assertFalse(service.equals(new KafkaItemInfo("foobar", 5, "zulu", 2, 500L, 4500L)));
		assertFalse(service.equals(new KafkaItemInfo("barbar", 8, "gamu", 3, 800L, 4500L)));
	}

}
