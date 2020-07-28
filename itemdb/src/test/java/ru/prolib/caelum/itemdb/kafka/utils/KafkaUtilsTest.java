package ru.prolib.caelum.itemdb.kafka.utils;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.log4j.BasicConfigurator;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.kafka.ItemIterator;
import ru.prolib.caelum.itemdb.kafka.KafkaItem;
import ru.prolib.caelum.itemdb.kafka.KafkaItemInfo;
import ru.prolib.caelum.itemdb.kafka.KafkaItemSerdes;
import ru.prolib.caelum.itemdb.kafka.SeamlessConsumerRecordIterator;

@SuppressWarnings("unchecked")
public class KafkaUtilsTest {
	static final String SYMBOL;
	static final int HASH_CODE;
	
	static {
		SYMBOL = "bumba";
		HASH_CODE = Utils.toPositive(Utils.murmur2(KafkaItemSerdes.keySerde().serializer().serialize("topic", SYMBOL)));
	}
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	@Rule
	public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaConsumer<String, KafkaItem> consumerMock;
	AdminClient adminMock;
	KafkaStreams streamsMock;
	Clock clockMock;
	KafkaUtils service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		adminMock = control.createMock(AdminClient.class);
		streamsMock = control.createMock(KafkaStreams.class);
		clockMock = control.createMock(Clock.class);
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
	public void testGetSymbolPartition() {
		// good symbols cases for 1, 2 and 4 partitions
		assertEquals(0, service.getSymbolPartition("zoo@gap", 2));
		assertEquals(1, service.getSymbolPartition("zoo@lol", 2));
		assertEquals(0, service.getSymbolPartition("zoo@foo", 2));
		assertEquals(1, service.getSymbolPartition("zoo@bar", 2));
		
		assertEquals(0, service.getSymbolPartition("zoo@gap", 4));
		assertEquals(1, service.getSymbolPartition("zoo@lol", 4));
		assertEquals(2, service.getSymbolPartition("zoo@foo", 4));
		assertEquals(3, service.getSymbolPartition("zoo@bar", 4));
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
		
		KafkaItemInfo actual = service.getItemInfo(consumerMock, "zulu24", SYMBOL);
		
		control.verify();
		KafkaItemInfo expected = new KafkaItemInfo("zulu24", 8, SYMBOL, expected_partition, 25L, 802L);
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
		
		KafkaItemInfo actual = service.getItemInfo(consumerMock, "bubba", SYMBOL);
		
		control.verify();
		KafkaItemInfo expected = new KafkaItemInfo("bubba", 1, SYMBOL, 0, null, 854L);
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
		
		KafkaItemInfo actual = service.getItemInfo(consumerMock, "bubba", SYMBOL);
		
		control.verify();
		KafkaItemInfo expected = new KafkaItemInfo("bubba", 1, SYMBOL, 0, 504L, null);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testCreateIteratorStub() {
		IItemIterator actual = service.createIteratorStub(consumerMock,
				new KafkaItemInfo("boo", 1, "foo", 0, 400L, 800L), 150, 718256L);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemIterator.class)));
		ItemIterator o = (ItemIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new KafkaItemInfo("boo", 1, "foo", 0, 400L, 800L), o.getItemInfo());
		assertEquals(150, o.getLimit());
		assertEquals(Long.valueOf(718256L), o.getEndTime());
		Iterator<ConsumerRecord<String, KafkaItem>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(IteratorStub.class)));
		assertEquals(new IteratorStub<>(), it);
	}
	
	@Test
	public void testCreateIteratorStub_ShouldBeOkIfEndTimeIsNull() {
		IItemIterator actual = service.createIteratorStub(consumerMock,
				new KafkaItemInfo("boo", 1, "foo", 0, 400L, 800L), 150, null);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemIterator.class)));
		ItemIterator o = (ItemIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new KafkaItemInfo("boo", 1, "foo", 0, 400L, 800L), o.getItemInfo());
		assertEquals(150, o.getLimit());
		assertNull(o.getEndTime());
		Iterator<ConsumerRecord<String, KafkaItem>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(IteratorStub.class)));
		assertEquals(new IteratorStub<>(), it);
	}
	
	@Test
	public void testCreateIterator() {
		IItemIterator actual = service.createIterator(consumerMock,
				new KafkaItemInfo("bug", 5, "juk", 3, 100L, 800L), 750, 2889000187L, clockMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemIterator.class)));
		ItemIterator o = (ItemIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new KafkaItemInfo("bug", 5, "juk", 3, 100L, 800L), o.getItemInfo());
		assertEquals(750, o.getLimit());
		assertEquals(Long.valueOf(2889000187L), o.getEndTime());
		Iterator<ConsumerRecord<String, KafkaItem>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(SeamlessConsumerRecordIterator.class)));
		assertSame(consumerMock, ((SeamlessConsumerRecordIterator<String, KafkaItem>) it).getConsumer());
		assertSame(clockMock, ((SeamlessConsumerRecordIterator<String, KafkaItem>) it).getClock());
	}
	
	@Test
	public void testCreateIterator_ShouldBeOkIfEndTimeIsNull() {
		IItemIterator actual = service.createIterator(consumerMock,
				new KafkaItemInfo("pop", 4, "gap", 1, null, null), 240, null, clockMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemIterator.class)));
		ItemIterator o = (ItemIterator) actual;
		assertSame(consumerMock, o.getConsumer());
		assertEquals(new KafkaItemInfo("pop", 4, "gap", 1, null, null), o.getItemInfo());
		assertEquals(240, o.getLimit());
		assertNull(o.getEndTime());
		Iterator<ConsumerRecord<String, KafkaItem>> it = o.getSourceIterator();
		assertThat(it, is(instanceOf(SeamlessConsumerRecordIterator.class)));
		assertSame(consumerMock, ((SeamlessConsumerRecordIterator<String, KafkaItem>) it).getConsumer());
		assertSame(clockMock, ((SeamlessConsumerRecordIterator<String, KafkaItem>) it).getClock());
	}
	
	@Test
	public void testGetOffset_ShouldReturnOffsetFromKafkaIfAvailable() {
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
	public void testGetOffset_ShouldReturnDefaultOffsetIfNoOffsetFromKafkaAvailable() {
		Map<TopicPartition, Long> map_arg = new HashMap<>();
		map_arg.put(new TopicPartition("foo", 2), 28866612L);
		expect(consumerMock.offsetsForTimes(map_arg)).andReturn(new HashMap<>());
		control.replay();
		
		assertEquals(150L, service.getOffset(consumerMock, new TopicPartition("foo", 2), 28866612L, 150L));
		
		control.verify();
	}
	
	@Test
	public void testGetOffset_ShouldReturnDefaultOffsetIfTimestampIsNull() {
		control.replay();
		
		assertEquals(894L, service.getOffset(consumerMock, new TopicPartition("foo", 2), null, 894L));
		
		control.verify();
	}
	
	@Test
	public void testCreateConsumer() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, "zumba-19");
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		KafkaConsumer<String, KafkaItem> actual = service.createConsumer(conf);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateProducer() {
		Properties conf = new Properties();
		conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
		conf.put(ProducerConfig.ACKS_CONFIG, "all");
		
		KafkaProducer<String, KafkaItem> actual = service.createProducer(conf);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateAdmin() {
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
		
		AdminClient actual = service.createAdmin(conf);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaAdminClient.class)));
	}
	
	@Test
	public void testCreateStreams() {
		StreamsBuilder sb = new StreamsBuilder();
		sb.stream("test-input").to("target-stream");
		Topology topology = sb.build();
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8082");
		
		KafkaStreams actual = service.createStreams(topology, conf);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateStreamsService() {
		AbstractConfig configMock = control.createMock(AbstractConfig.class);
		
		IService actual = service.createStreamsService(streamsMock, "foo", configMock);
		
		IService expected = new KafkaStreamsService(streamsMock, "foo", configMock);
		assertEquals(expected, actual);
	}
	
	static class MapBuilder<K, V> {
		private final Map<K, V> result = new LinkedHashMap<>();
		
		public MapBuilder<K, V> put(K key, V val) {
			result.put(key, val);
			return this;
		}
		
		public Map<K, V> build() {
			return new LinkedHashMap<>(result);
		}
	}
	
	@Test
	public void testDeleteRecords_NoErrors() throws Exception {
		// 1) requesting topic partitions info
		DescribeTopicsResult descrTopicsResMock = control.createMock(DescribeTopicsResult.class);
		expect(adminMock.describeTopics(Arrays.asList("foobar"))).andReturn(descrTopicsResMock);
		KafkaFuture<Map<String, TopicDescription>> futMock1 = control.createMock(KafkaFuture.class);
		expect(descrTopicsResMock.all()).andReturn(futMock1);
		expect(futMock1.get(10000L, TimeUnit.MILLISECONDS)).andReturn(new MapBuilder<String, TopicDescription>()
				.put("foobar", new TopicDescription("foobar", false, Arrays.asList(
						new TopicPartitionInfo(0, null, new ArrayList<>(), new ArrayList<>()),
						new TopicPartitionInfo(1, null, new ArrayList<>(), new ArrayList<>()),
						new TopicPartitionInfo(2, null, new ArrayList<>(), new ArrayList<>())
					)))
				.build());
		// 2) requesting for latest offset for each partition
		ListOffsetsResult listOffsetsResMock = control.createMock(ListOffsetsResult.class);
		Capture<Map<TopicPartition, OffsetSpec>> cap = newCapture();
		expect(adminMock.listOffsets(capture(cap))).andReturn(listOffsetsResMock);
		KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> futMock2 = control.createMock(KafkaFuture.class);
		expect(listOffsetsResMock.all()).andReturn(futMock2);
		ListOffsetsResultInfo offsetInfoMock1 = control.createMock(ListOffsetsResultInfo.class);
		ListOffsetsResultInfo offsetInfoMock2 = control.createMock(ListOffsetsResultInfo.class);
		ListOffsetsResultInfo offsetInfoMock3 = control.createMock(ListOffsetsResultInfo.class);
		expect(futMock2.get(10000L, TimeUnit.MILLISECONDS))
			.andReturn(new MapBuilder<TopicPartition, ListOffsetsResultInfo>()
					.put(new TopicPartition("foobar", 0), offsetInfoMock1)
					.put(new TopicPartition("foobar", 1), offsetInfoMock2)
					.put(new TopicPartition("foobar", 2), offsetInfoMock3)
				.build());
		expect(offsetInfoMock1.offset()).andStubReturn(1798L);
		expect(offsetInfoMock2.offset()).andStubReturn(8261L);
		expect(offsetInfoMock3.offset()).andStubReturn(5712L);
		// 3) deleting records for each partition
		DeleteRecordsResult deleteResMock = control.createMock(DeleteRecordsResult.class);
		expect(adminMock.deleteRecords(new MapBuilder<TopicPartition, RecordsToDelete>()
				.put(new TopicPartition("foobar", 0), RecordsToDelete.beforeOffset(1798L))
				.put(new TopicPartition("foobar", 1), RecordsToDelete.beforeOffset(8261L))
				.put(new TopicPartition("foobar", 2), RecordsToDelete.beforeOffset(5712L))
				.build()))
			.andReturn(deleteResMock);
		KafkaFuture<Void> futMock3 = control.createMock(KafkaFuture.class);
		expect(deleteResMock.all()).andReturn(futMock3);
		expect(futMock3.get(10000L, TimeUnit.MILLISECONDS)).andReturn(null);
		control.replay();
		
		service.deleteRecords(adminMock, "foobar", 10000L);
		
		control.verify();
		assertEquals("LatestSpec", cap.getValue().get(new TopicPartition("foobar", 0)).getClass().getSimpleName());
		assertEquals("LatestSpec", cap.getValue().get(new TopicPartition("foobar", 1)).getClass().getSimpleName());
		assertEquals("LatestSpec", cap.getValue().get(new TopicPartition("foobar", 2)).getClass().getSimpleName());
	}
	
}
