package ru.prolib.caelum.itemdb.kafka;

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Seamless record iterator makes an iterator based on Kafka consumer. It does not perform
 * any checks - consumer has to be subscribed on appropriate topics. It does not manage any
 * resources - consumer should be explicitly closed after usage. It just iterates through
 * elements returned from pooling loop.
* <p>
 * @param <K> - key type
 * @param <V> - value type
 */
public class SeamlessConsumerRecordIterator<K, V> implements Iterator<ConsumerRecord<K, V>> {
	static final Logger logger = LoggerFactory.getLogger(SeamlessConsumerRecordIterator.class);
	/**
	 * Default wait duration while polling records.
	 */
	public static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(1);
	/**
	 * Timeout while waiting for new records.
	 * If no records were came in this time then exception will be thrown.
	 */
	public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
	private final KafkaConsumer<K, V> consumer;
	private final KafkaItemInfo itemInfo;
	private final Clock clock;
	private final TopicPartition topicPartition;
	private Iterator<ConsumerRecord<K, V>> it;
	
	public SeamlessConsumerRecordIterator(KafkaConsumer<K, V> consumer,
			KafkaItemInfo itemInfo,
			Clock clock)
	{
		this.consumer = consumer;
		this.itemInfo = itemInfo;
		this.clock = clock;
		this.topicPartition = itemInfo.toTopicPartition();
	}
	
	public KafkaConsumer<K, V> getConsumer() {
		return consumer;
	}
	
	public KafkaItemInfo getItemInfo() {
		return itemInfo;
	}
	
	public Clock getClock() {
		return clock;
	}

	private long posAtLastHasNaxtCall;
	private long posAtLastNextCall;
	
	@Override
	public boolean hasNext() {
		if ( it != null && it.hasNext() ) {
			return true;
		}
		return (posAtLastHasNaxtCall = consumer.position(topicPartition)) < itemInfo.getEndOffset();
	}

	@Override
	public ConsumerRecord<K, V> next() {
		final Duration d = DEFAULT_POLL_DURATION;
		final Duration timeout = DEFAULT_TIMEOUT;
		Duration no_data = null;
		for ( ;; ) {
			if ( it == null || ! it.hasNext() ) {
				if ( (posAtLastNextCall = consumer.position(topicPartition)) >= itemInfo.getEndOffset() ) {
					throw new NoSuchElementException(new StringBuilder()
							.append("posAtLastHasNextCall=").append(posAtLastHasNaxtCall).append(" ")
							.append("posAtLastNextCall=").append(posAtLastNextCall)
							.toString());
				}
				
				long t_start = clock.millis();
				ConsumerRecords<K, V> cr = consumer.poll(d);
				it = cr.iterator();
				if ( it.hasNext() == false ) {
					long t_used = clock.millis() - t_start;
					no_data = no_data == null ? Duration.ofMillis(t_used) : no_data.plusMillis(t_used);
					if ( no_data.compareTo(timeout) >= 0 ) {
						throw new IllegalStateException(new TimeoutException());
					}
					continue;
				}
				no_data = null;
			}
			
			if ( it.hasNext() ) {
				return it.next();
			}
		}
	}

}
