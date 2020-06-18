package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.itemdb.kafka.SeamlessConsumerRecordIterator;

@SuppressWarnings("unchecked")
public class SeamlessConsumerRecordIteratorTest {
	static TopicPartition part = new TopicPartition("foobar", 0);
	
	static ConsumerRecord<String, Integer> CR(String key, Integer value) {
		return new ConsumerRecord<>(part.topic(), part.partition(), 0, key, value);
	}
	
	static ConsumerRecords<String, Integer> CRS(List<ConsumerRecord<String, Integer>> records) {
		Map<TopicPartition, List<ConsumerRecord<String, Integer>>> map = new HashMap<>();
		map.put(part, records);
		return new ConsumerRecords<>(map);
	}
	
	static KeyValue<String, Integer> KV(String key, Integer value) {
		return new KeyValue<>(key, value);
	}
	
	static KeyValue<String, Integer> KV(ConsumerRecord<String, Integer> record) {
		return KV(record.key(),record.value());
	}
	
	IMocksControl control;
	KafkaConsumer<String, Integer> consumerMock;
	SeamlessConsumerRecordIterator<String, Integer> service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		service = new SeamlessConsumerRecordIterator<>(consumerMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(consumerMock, service.getConsumer());
	}
	
	@Test
	public void testHasNext_IsAlwaysTrue() {
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
	}
	
	@Test
	public void testNext() {
		Duration d = Duration.ofSeconds(1);
		// The first block
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(
				CR("foo",  1),
				CR("bar",  2),
				CR("foo",  5)
			)));
		// The second block is empty
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		// The block #3
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(
				CR("one", 29),
				CR("bad", 12),
				CR("goo", 86)
			)));
		// The block #4 is empty
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		// The block #5
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(
				CR("foo", 46),
				CR("bug", 44)
			)));
		// etc...
		control.replay();
		
		List<KeyValue<String, Integer>> actual = new ArrayList<>();
		for ( int i = 0; i < 8; i ++ ) {
			actual.add(KV(service.next()));
		}
		
		control.verify();
		List<KeyValue<String, Integer>> expected = Arrays.asList(
				KV("foo",  1),
				KV("bar",  2),
				KV("foo",  5),
				KV("one", 29),
				KV("bad", 12),
				KV("goo", 86),
				KV("foo", 46),
				KV("bug", 44)
			);
		assertEquals(expected, actual);
	}

}
