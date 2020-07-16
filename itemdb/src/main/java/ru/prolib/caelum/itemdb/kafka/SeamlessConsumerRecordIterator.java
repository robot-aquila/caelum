package ru.prolib.caelum.itemdb.kafka;

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
	private final Clock clock;
	private Iterator<ConsumerRecord<K, V>> it;
	
	public SeamlessConsumerRecordIterator(KafkaConsumer<K, V> consumer, Clock clock) {
		this.consumer = consumer;
		this.clock = clock;
	}
	
	public KafkaConsumer<K, V> getConsumer() {
		return consumer;
	}
	
	public Clock getClock() {
		return clock;
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public ConsumerRecord<K, V> next() {
		final Duration d = DEFAULT_POLL_DURATION;
		final Duration timeout = DEFAULT_TIMEOUT;
		Duration no_data = null;
		for ( ;; ) {
			if ( it == null || ! it.hasNext() ) {
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
