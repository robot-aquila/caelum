package ru.prolib.caelum.itemdb.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Item;

public class KafkaUtils {
	
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

}
