package ru.prolib.caelum.test;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBCandleMutable;
import ru.prolib.caelum.core.LBTradeAggregator;

public class TradeAggregator {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "caelum-trade-aggregator");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:32768");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CaelumSerdes.ILBTrade().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		
		KTable<Windowed<String>, LBCandleMutable> result = builder.<String, ILBTrade>stream("caelum-trades")
			.groupByKey()
			.windowedBy(TimeWindows.of(Duration.ofMinutes(1L)))
			.aggregate(LBCandleMutable::new, new LBTradeAggregator(),
				Materialized.<String, LBCandleMutable, WindowStore<Bytes, byte[]>>as("caelum-candle-m1-store")
					.withValueSerde(CaelumSerdes.LBCandleMutable()));
		result.toStream()
			.foreach((key, value) -> {
				System.out.println("S:" + key.key() + " T:" + key.window().startTime() + " V:" + value);
			});

		Topology topology = builder.build();
		
		KafkaStreams streams = new KafkaStreams(topology, props);
		System.out.println(topology.describe());
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				streams.close();
			}
		});
	}

}
