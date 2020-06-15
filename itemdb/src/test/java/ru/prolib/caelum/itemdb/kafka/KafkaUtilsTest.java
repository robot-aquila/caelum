package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.KafkaConsumer;
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

}
