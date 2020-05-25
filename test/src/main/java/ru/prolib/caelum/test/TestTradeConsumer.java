package ru.prolib.caelum.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBTradeDeserializer;

public class TestTradeConsumer {
	
	public static void main(String[] args) throws Exception {
		new TestTradeConsumer().run(args);
	}
	
	public void run(String[] args) throws Exception {
		//BasicConfigurator.configure();
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.99.100:32768");
		props.put("group.id", "test-trade-consumer");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", LBTradeDeserializer.class);
		KafkaConsumer<String, ILBTrade> consumer = new KafkaConsumer<>(props);
		
		final CountDownLatch finished = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override public void run() {
				finished.countDown();
			}
		});
		
		consumer.subscribe(Arrays.asList("caelum-trades"));
		while ( finished.getCount() > 0 ) {
			ConsumerRecords<String, ILBTrade> records = consumer.poll(Duration.ofSeconds(1));
			for ( ConsumerRecord<String, ILBTrade> record : records ) {
				System.out.println("  Time: " + record.timestamp());
				System.out.println("Symbol: " + record.key());
				System.out.println(" Trade: " + record.value());
			}
		}
		System.out.println("Exiting");
		consumer.close();
		
	}

}
