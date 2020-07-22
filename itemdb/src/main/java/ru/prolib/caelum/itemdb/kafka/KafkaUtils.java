package ru.prolib.caelum.itemdb.kafka;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.IItemIterator;

public class KafkaUtils {
	private static final KafkaUtils instance = new KafkaUtils();
	
	public static KafkaUtils getInstance() {
		return instance;
	}
	
	public int getSymbolPartition(String symbol, int num_partitions) {
		byte[] key_bytes = KafkaItemSerdes.keySerde().serializer().serialize(null, symbol);
		int partition = Utils.toPositive(Utils.murmur2(key_bytes)) % num_partitions;
		return partition;
	}
	
	public KafkaItemInfo getItemInfo(KafkaConsumer<String, KafkaItem> consumer, String topic, String symbol) {
		Set<Integer> partitions = consumer.partitionsFor(topic)
				.stream()
				.map(x -> x.partition())
				.collect(Collectors.toSet());
		int num_partitions = partitions.size();
		int partition = getSymbolPartition(symbol, num_partitions);
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
		return new KafkaItemInfo(topic, num_partitions, symbol, partition, start_offset, end_offset);
	}
	
	public IItemIterator createIteratorStub(KafkaConsumer<String, KafkaItem> consumer,
			KafkaItemInfo item_info, int limit, Long end_time)
	{
		return new ItemIterator(consumer, new IteratorStub<>(), item_info, limit, end_time);
	}
	
	public IItemIterator createIterator(KafkaConsumer<String, KafkaItem> consumer,
			KafkaItemInfo item_info, int limit, Long end_time, Clock clock)
	{
		return new ItemIterator(consumer, new SeamlessConsumerRecordIterator<>(consumer, clock),
				item_info, limit, end_time);
	}
	
	public long getOffset(KafkaConsumer<?, ?> consumer, TopicPartition tp, Long timestamp, long default_offset) {
		if ( timestamp == null ) {
			return default_offset;
		}
		Map<TopicPartition, Long> m = new HashMap<>();
		m.put(tp, timestamp);
		OffsetAndTimestamp x = consumer.offsetsForTimes(m).get(tp);
		return x == null ? default_offset : x.offset();
	}
	
	public KafkaConsumer<String, KafkaItem> createConsumer(Properties props) {
		return new KafkaConsumer<>(props, KafkaItemSerdes.keySerde().deserializer(),
				KafkaItemSerdes.itemSerde().deserializer());
	}

	public KafkaProducer<String, KafkaItem> createProducer(Properties props) {
		return new KafkaProducer<>(props, KafkaItemSerdes.keySerde().serializer(),
				KafkaItemSerdes.itemSerde().serializer());
	}
	
	public AdminClient createAdmin(Properties props) {
		return AdminClient.create(props);
	}
	
	/**
	 * Delete all records of kafka topic.
	 * <p>
	 * @param admin - admin client instance
	 * @param topic - topic
	 * @param timeout - timeout of separate blocking operation in milliseconds
	 * @throws IllegalStateException - an error occurred
	 */
	public void deleteRecords(AdminClient admin, String topic, long timeout) throws IllegalStateException {
		try {
			DescribeTopicsResult r = admin.describeTopics(Arrays.asList(topic));
			TopicDescription desc = r.all().get(timeout, TimeUnit.MILLISECONDS).get(topic);
			if ( desc != null ) {
				List<TopicPartition> partitions = desc.partitions().stream()
						.map((tpi) -> new TopicPartition(topic, tpi.partition()))
						.collect(Collectors.toList());
				Map<TopicPartition, RecordsToDelete> to_delete = admin
					.listOffsets(partitions.stream().collect(Collectors.toMap(x->x, x->OffsetSpec.latest())))
					.all()
					.get(timeout, TimeUnit.MILLISECONDS)
					.entrySet()
					.stream()
					.collect(Collectors.toMap(x->x.getKey(), x->RecordsToDelete.beforeOffset(x.getValue().offset())));
				admin.deleteRecords(to_delete).all().get(timeout, TimeUnit.MILLISECONDS);
			}
		} catch ( TimeoutException|ExecutionException|InterruptedException e ) {
			throw new IllegalStateException("Error deleting records from topic: " + topic, e);
		}
	}

}
