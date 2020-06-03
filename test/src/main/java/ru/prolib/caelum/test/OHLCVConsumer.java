package ru.prolib.caelum.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.ILBOHLCV;
import ru.prolib.caelum.core.LBOHLCVDeserializer;

public class OHLCVConsumer implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(OHLCVConsumer.class);
	
	public static void main(String[] args) throws Exception {
		final ExecutorService executor = Executors.newFixedThreadPool(1);
		final OHLCVConsumerConfig config = new OHLCVConsumerConfig();
		config.load(OHLCVConsumerConfig.DEFAULT_CONFIG_FILE, args.length > 0 ? args[0] : null);
		final OHLCVConsumer consumer = new OHLCVConsumer(config);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			consumer.shutdown();
			executor.shutdown();
			try {
				executor.awaitTermination(15L, TimeUnit.SECONDS);
			} catch ( InterruptedException e ) {
				logger.error("Unexpected interruption: ", e);
			}			
		}));
		executor.submit(consumer);
	}
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<Windowed<String>, ILBOHLCV> consumer;
	private final OHLCVConsumerConfig config;
	
	public OHLCVConsumer(OHLCVConsumerConfig config) {
		this.consumer = new KafkaConsumer<>(config.getKafkaProperties(),
				new TimeWindowedDeserializer<>(new StringDeserializer()), new LBOHLCVDeserializer());
		this.config = config;
	}
	
	protected void consumeRecord(ConsumerRecord<Windowed<String>, ILBOHLCV> record) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("partition", record.partition());
		map.put("offset", record.offset());
		map.put("key", record.key().key());
		map.put("time", record.key().window().startTime());
		map.put("val", record.value());
		System.out.println(map);
	}
	
	@Override
	public void run() {
		final String topic = config.getSourceTopic();
		logger.debug("Started for topic: {}", topic);
		Duration poll_interval = Duration.ofSeconds(10L);
		try {
			consumer.subscribe(Arrays.asList(topic));
			while ( closed.get() == false ) {
				ConsumerRecords<Windowed<String>, ILBOHLCV> records = consumer.poll(poll_interval);
				for ( ConsumerRecord<Windowed<String>, ILBOHLCV> record : records ) {
					consumeRecord(record);
				}
			}
		} catch ( WakeupException e ) {
			if ( ! closed.get() ) {
				logger.error("Unexpected exception: ", e);
				throw e;
			}
		} finally {
			consumer.close();
		}
		logger.debug("Stopped for topic: {}", config.getSourceTopic());
	}
	
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
	
}
