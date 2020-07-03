package ru.prolib.caelum.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import ru.prolib.caelum.itemdb.kafka.KafkaItem;
import ru.prolib.caelum.itemdb.kafka.KafkaItemSerdes;

public class ItemConsumer {
	
	public static void main(String[] args) throws Exception {
		new ItemConsumer().run(args);
	}
	
	public void run(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.99.100:32768");
		props.put("group.id", "caelum-item-consumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		KafkaConsumer<String, KafkaItem> consumer = new KafkaConsumer<>(props,
			KafkaItemSerdes.keySerde().deserializer(), KafkaItemSerdes.itemSerde().deserializer());
		
		final CountDownLatch finished = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override public void run() {
				finished.countDown();
			}
		});
		
		consumer.subscribe(Arrays.asList("caelum-item"));
		while ( finished.getCount() > 0 ) {
			ConsumerRecords<String, KafkaItem> records = consumer.poll(Duration.ofSeconds(1));
			for ( ConsumerRecord<String, KafkaItem> record : records ) {
				System.out.println("  Time: " + record.timestamp());
				System.out.println("Symbol: " + record.key());
				System.out.println(" Value: " + record.value());
			}
			// Можно кидать искюлючения по ходу вывода. Все норм будет
		}
		System.out.println("Exiting");
		consumer.close();
	}

}
