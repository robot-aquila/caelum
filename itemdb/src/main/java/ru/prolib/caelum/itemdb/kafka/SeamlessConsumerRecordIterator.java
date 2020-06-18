package ru.prolib.caelum.itemdb.kafka;

import java.time.Duration;
import java.util.Iterator;

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
	private final KafkaConsumer<K, V> consumer;
	private Iterator<ConsumerRecord<K, V>> it;
	
	public SeamlessConsumerRecordIterator(KafkaConsumer<K, V> consumer) {
		this.consumer = consumer;
	}
	
	public KafkaConsumer<K, V> getConsumer() {
		return consumer;
	}

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public ConsumerRecord<K, V> next() {
		for ( ;; ) {
			if ( it == null || ! it.hasNext() ) {
				ConsumerRecords<K, V> cr = consumer.poll(Duration.ofSeconds(1));
				if ( cr.isEmpty() ) {
					continue;
				}
				it = cr.iterator();
			}
			if ( it.hasNext() ) {
				return it.next();
			}
		}
	}

}
