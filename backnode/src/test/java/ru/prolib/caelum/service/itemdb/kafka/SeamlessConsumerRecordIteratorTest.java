package ru.prolib.caelum.service.itemdb.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
	
	static ConsumerRecords<String, Integer> CRS(ConsumerRecord<String, Integer> ...records) {
		return CRS(Arrays.asList(records));
	}
	
	static KeyValue<String, Integer> KV(String key, Integer value) {
		return new KeyValue<>(key, value);
	}
	
	static KeyValue<String, Integer> KV(ConsumerRecord<String, Integer> record) {
		return KV(record.key(),record.value());
	}
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaConsumer<String, Integer> consumerMock;
	KafkaItemInfo itemInfo;
	Clock clockMock;
	SeamlessConsumerRecordIterator<String, Integer> service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		itemInfo = new KafkaItemInfo("topic", 8, "my@bar", 3, 765L, 987L);
		clockMock = control.createMock(Clock.class);
		service = new SeamlessConsumerRecordIterator<>(consumerMock, itemInfo, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertEquals(consumerMock, service.getConsumer());
		assertEquals(itemInfo, service.getItemInfo());
		assertEquals(clockMock, service.getClock());
	}
	
	@Test
	public void testHasNext() {
		expect(consumerMock.position(new TopicPartition("topic", 3)))
			.andReturn(985L)
			.andReturn(986L)
			.andReturn(987L)
			.andReturn(988L);
		control.replay();
		
		assertTrue(service.hasNext());
		assertTrue(service.hasNext());
		assertFalse(service.hasNext());
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testHasNext_ShouldReturnTrueIfIteratorDefinedAndHasNext() {
		expect(clockMock.millis()).andStubReturn(1000L);
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(100L);
		expect(consumerMock.poll(Duration.ofSeconds(1))).andReturn(CRS(CR("foo", 1), CR("bar", 2), CR("foo", 5)));
		control.replay();
		assertEquals(KV("foo", 1), KV(service.next()));
		control.resetToStrict();
		control.replay();
		
		assertTrue(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testNext() {
		long does_not_matter = 1000L;
		Duration d = Duration.ofSeconds(1);
		// The first block
		expect(clockMock.millis()).andStubReturn(does_not_matter);
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(100L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(
				CR("foo",  1),
				CR("bar",  2),
				CR("foo",  5)
			)));
		// The second block is empty
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(103L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		// The block #3
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(103L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(
				CR("one", 29),
				CR("bad", 12),
				CR("goo", 86)
			)));
		// The block #4 is empty
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(103L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		// The block #5
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(103L);
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
	
	@Test
	public void testNext_ThrowsIfTimeoutReadingRecords() {
		expect(consumerMock.position(new TopicPartition("topic", 3))).andStubReturn(100L);
		Duration d = Duration.ofSeconds(1);
		expect(clockMock.millis()).andReturn(5000L);
		// block #1 is OK, but it's the only block received
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList(CR("foo",  1), CR("bar",  2), CR("foo",  5))));
		// block #2
		expect(clockMock.millis()).andReturn( 5000L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		expect(clockMock.millis()).andReturn( 6000L); // +1s
		// block #3
		expect(clockMock.millis()).andReturn( 6000L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		expect(clockMock.millis()).andReturn( 7000L); // +1s
		// block #4
		expect(clockMock.millis()).andReturn( 7000L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		expect(clockMock.millis()).andReturn( 9000L); // +2s
		// block #5 it's time
		expect(clockMock.millis()).andReturn( 9000L);
		expect(consumerMock.poll(d)).andReturn(CRS(Arrays.asList()));
		expect(clockMock.millis()).andReturn(25000L);
		control.replay();
		service.next();
		service.next();
		service.next();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.next());
		assertThat(e.getCause(), is(instanceOf(TimeoutException.class)));
	}
	
	@Test
	public void testNext_ThrowsIfOutOfKnownRange() {
		expect(consumerMock.position(new TopicPartition("topic", 3))).andReturn(987L);
		control.replay();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
		
		control.verify();
	}

}
