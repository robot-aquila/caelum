package ru.prolib.caelum.itemdb.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.IItemDataIterator;

public class KafkaUtils {
	private static final KafkaUtils instance = new KafkaUtils();
	
	public static KafkaUtils getInstance() {
		return instance;
	}
	
	public ItemInfo getItemInfo(KafkaConsumer<String, Item> consumer, String topic, String symbol) {
		Set<Integer> partitions = consumer.partitionsFor(topic)
				.stream()
				.map(x -> x.partition())
				.collect(Collectors.toSet());
		byte[] key_bytes = CaelumSerdes.keySerde().serializer().serialize(topic, symbol);
		int num_partitions = partitions.size();
		int partition = Utils.toPositive(Utils.murmur2(key_bytes)) % num_partitions;
		if ( ! partitions.contains(partition) ) {
			throw new IllegalStateException(new StringBuilder()
					.append("Expected partition not found: topic=")
					.append(topic)
					.append(" partition=")
					.append(partition)
					.toString());
		}
		TopicPartition topic_partition = new TopicPartition(topic, partition);
		List<TopicPartition> topic_partitions = Arrays.asList(topic_partition);
		Map<TopicPartition, Long> r = null;
		r = consumer.beginningOffsets(topic_partitions); Long start_offset = r.get(topic_partition);
		r = consumer.endOffsets(topic_partitions);		 Long end_offset = r.get(topic_partition);
		return new ItemInfo(topic, num_partitions, symbol, partition, start_offset, end_offset);
	}
	
	public IItemDataIterator createIteratorStub(KafkaConsumer<String, Item> consumer,
			ItemInfo item_info, long limit, long end_time)
	{
		return new ItemDataIterator(consumer, new IteratorStub<>(), item_info, limit, end_time);
	}
	
	public IItemDataIterator createIterator(KafkaConsumer<String, Item> consumer,
			ItemInfo item_info, long limit, long end_time)
	{
		return new ItemDataIterator(consumer, new SeamlessConsumerRecordIterator<>(consumer),
				item_info, limit, end_time);
	}
	
	public long getOffset(KafkaConsumer<?, ?> consumer, TopicPartition tp, long timestamp, long default_offset) {
		Map<TopicPartition, Long> m = new HashMap<>();
		m.put(tp, timestamp);
		OffsetAndTimestamp x = consumer.offsetsForTimes(m).get(tp);
		return x == null ? default_offset : x.offset();
	}
	
	public KafkaConsumer<String, Item> createConsumer(Properties props) {
		return new KafkaConsumer<>(props, CaelumSerdes.keySerde().deserializer(),
				CaelumSerdes.itemSerde().deserializer());
	}


}
