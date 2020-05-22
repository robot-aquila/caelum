package ru.prolib.caelum.test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SymbolList {
	final static Logger logger = LoggerFactory.getLogger(SymbolList.class);

	public static void main(String[] args) {
		//BasicConfigurator.configure();
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-stream-processor");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:32768");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, String> stream = builder.stream("my-input-stream");
		stream.groupBy((k, v) -> {
				System.out.println("groupBy: " + k + ":" + v);
				return v;
			}).count().filter((k, v) -> {
				System.out.println("filter: " + k + ":" + v + " ? " + (v == 1));
				return v == 1;
			}).toStream().to("my-output-stream");
		
		final Topology topology = builder.build();
		
		System.out.println(topology.describe());
		
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch finished = new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("test-stream-processor-shutdown") {
			@Override
			public void run() {
				streams.close();
				finished.countDown();
			}
		});
		
		try {
			streams.start();
			finished.await();
		} catch ( Throwable e ) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
		System.exit(0);
	}

}
