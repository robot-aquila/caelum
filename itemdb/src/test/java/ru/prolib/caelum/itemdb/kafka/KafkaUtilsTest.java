package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.IItemDataIterator;

@SuppressWarnings("unchecked")
public class KafkaUtilsTest {
	static final String SYMBOL;
	static final int HASH_CODE;
	
	static {
		SYMBOL = "bumba";
		HASH_CODE = Utils.toPositive(Utils.murmur2(CaelumSerdes.keySerde().serializer().serialize("topic", SYMBOL)));
	}
	
	@Rule
	public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaConsumer<String, Item> consumerMock;
	KafkaUtils service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		service = new KafkaUtils();
	}
	
	@Test
	public void testGetInstance() {
		KafkaUtils actual = KafkaUtils.getInstance();
		assertNotNull(actual);
		assertSame(actual, KafkaUtils.getInstance());
		assertSame(actual, KafkaUtils.getInstance());
		assertSame(actual, KafkaUtils.getInstance());
	}
	
	@Test
	public void testGetItemInfo_ThrowsIfPartitionNotExists() {
		int num_partitions = 4;
		int expected_partition = HASH_CODE % num_partitions;
		List<PartitionInfo> partitions = new ArrayList<>();
		for ( int i = 0; i < num_partitions; i ++ ) {
			int p = i >= expected_partition ? i + 1 : i; // shift at one up for all partitions starting expected one
			partitions.add(new PartitionInfo("zulu24", p, null, null, null));
		}
		expect(consumerMock.partitionsFor("zulu24")).andReturn(partitions);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage(new StringBuilder()
				.append("Expected partition not found: topic=zulu24 partition=")
				.append(expected_partition)
				.toString());

		service.getItemInfo(consumerMock, "zulu24", SYMBOL);
	}
	
	@Test
	public void testGetItemInfo_CompleteInfo() {
		int expected_partition = HASH_CODE % 8;
		expect(consumerMock.partitionsFor("zulu24")).andReturn(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7).stream()
				.map(x -> new PartitionInfo("zulu24", x, null, null, null))
				.collect(Collectors.toList()));
		expect(consumerMock.beginningOffsets(Arrays.asList(new TopicPartition("zulu24", expected_partition))))
			.andReturn(Arrays.asList(new KeyValue<Integer, Long>(expected_partition, 25L)).stream()
				.collect(Collectors.toMap(x -> new TopicPartition("zulu24", x.key), x -> x.value)));
		expect(consumerMock.endOffsets(Arrays.asList(new TopicPartition("zulu24", expected_partition))))
			.andReturn(Arrays.asList(new KeyValue<Integer, Long>(expected_partition, 802L)).stream()
				.collect(Collectors.toMap(x -> new TopicPartition("zulu24", x.key), x -> x.value)));
		control.replay();
		
		ItemInfo actual = service.getItemInfo(consumerMock, "zulu24", SYMBOL);
		
		control.verify();
		ItemInfo expected = new ItemInfo("zulu24", 8, SYMBOL, expected_partition, 25L, 802L);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetItemInfo_NoStartOffset() {
		expect(consumerMock.partitionsFor("bubba"))
			.andReturn(Arrays.asList(new PartitionInfo("bubba", 0, null, null, null)));
		expect(consumerMock.beginningOffsets(Arrays.asList(new TopicPartition("bubba", 0)))).andReturn(new HashMap<>());
		expect(consumerMock.endOffsets(Arrays.asList(new TopicPartition("bubba", 0))))
			.andReturn(Arrays.asList(new KeyValue<Integer, Long>(0, 854L)).stream()
				.collect(Collectors.toMap(x -> new TopicPartition("bubba", x.key), x -> x.value)));
		control.replay();
		
		ItemInfo actual = service.getItemInfo(consumerMock, "bubba", SYMBOL);
		
		control.verify();
		ItemInfo expected = new ItemInfo("bubba", 1, SYMBOL, 0, null, 854L);
		assertEquals(expected, actual);
	}

	@Test
	public void testGetItemInfo_NoEndOffset() {
		expect(consumerMock.partitionsFor("bubba"))
			.andReturn(Arrays.asList(new PartitionInfo("bubba", 0, null, null, null)));
		expect(consumerMock.beginningOffsets(Arrays.asList(new TopicPartition("bubba", 0))))
			.andReturn(Arrays.asList(new KeyValue<Integer, Long>(0, 504L)).stream()
				.collect(Collectors.toMap(x -> new TopicPartition("bubba", x.key), x -> x.value)));
		expect(consumerMock.endOffsets(Arrays.asList(new TopicPartition("bubba", 0)))).andReturn(new HashMap<>());
		control.replay();
		
		ItemInfo actual = service.getItemInfo(consumerMock, "bubba", SYMBOL);
		
		control.verify();
		ItemInfo expected = new ItemInfo("bubba", 1, SYMBOL, 0, 504L, null);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testCreateIteratorStub() {
		IItemDataIterator actual = service.createIteratorStub(consumerMock,
				new ItemInfo("boo", 1, "foo", 0, 400L, 800L), 150L, 718256L);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemDataIterator.class)));
		ItemDataIterator o = (ItemDataIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new ItemInfo("boo", 1, "foo", 0, 400L, 800L), o.getItemInfo());
		assertEquals(150L, o.getLimit());
		assertEquals(718256L, o.getEndTime());
		Iterator<ConsumerRecord<String, Item>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(IteratorStub.class)));
		assertEquals(new IteratorStub<>(), it);
	}
	
	@Test
	public void testCreateIterator() {
		IItemDataIterator actual = service.createIterator(consumerMock,
				new ItemInfo("bug", 5, "juk", 3, 100L, 800L), 750L, 2889000187L);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemDataIterator.class)));
		ItemDataIterator o = (ItemDataIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new ItemInfo("bug", 5, "juk", 3, 100L, 800L), o.getItemInfo());
		assertEquals(750L, o.getLimit());
		assertEquals(2889000187L, o.getEndTime());
		Iterator<ConsumerRecord<String, Item>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(SeamlessConsumerRecordIterator.class)));
		assertSame(consumerMock, ((SeamlessConsumerRecordIterator<String, Item>) it).getConsumer());
	}
	
	@Test
	public void testGetOffset_HasResult() {
		Map<TopicPartition, Long> map_arg = new HashMap<>();
		map_arg.put(new TopicPartition("foo", 2), 28866612L);
		Map<TopicPartition, OffsetAndTimestamp> map_res = new HashMap<>();
		map_res.put(new TopicPartition("foo", 2), new OffsetAndTimestamp(1000L, 28866620L));
		expect(consumerMock.offsetsForTimes(map_arg)).andReturn(map_res);
		control.replay();
		
		assertEquals(1000L, service.getOffset(consumerMock, new TopicPartition("foo", 2), 28866612L, 150L));
		
		control.verify();
	}
	
	@Test
	public void testGetOffset_NoResult() {
		Map<TopicPartition, Long> map_arg = new HashMap<>();
		map_arg.put(new TopicPartition("foo", 2), 28866612L);
		expect(consumerMock.offsetsForTimes(map_arg)).andReturn(new HashMap<>());
		control.replay();
		
		assertEquals(150L, service.getOffset(consumerMock, new TopicPartition("foo", 2), 28866612L, 150L));
		
		control.verify();
	}
	
	@Test
	public void testCreateConsumer() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, "zumba-19");
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		KafkaConsumer<String, Item> actual = service.createConsumer(conf);
		
		assertNotNull(actual);
	}

}
