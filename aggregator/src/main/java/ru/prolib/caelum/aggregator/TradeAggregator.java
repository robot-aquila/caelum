package ru.prolib.caelum.aggregator;

import java.io.IOException;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBCandleMutable;
import ru.prolib.caelum.core.LBTradeAggregator;

public class TradeAggregator {
	static final Logger logger = LoggerFactory.getLogger(TradeAggregator.class);

	public static void main(String[] args ) throws Exception {
		TradeAggregator service = new TradeAggregator();
		service.start(args.length > 0 ? args[0] : null);
	}
	
	public void start(String conf_file) throws IOException {
		TradeAggregatorConfig conf = new TradeAggregatorConfig();
		conf.loadFromResources();
		if ( conf_file != null ) {
			if ( ! conf.loadFromFile(conf_file) ) {
				throw new IOException("Error loading config: " + conf_file);
			}
		} else {
			conf.loadFromFile(TradeAggregatorConfig.DEFAULT_CONFIG_FILE);
		}

		logger.info("Starting up trade to aggregator for {}", conf.getString(TradeAggregatorConfig.AGGREGATION_PERIOD));
		conf.print(logger);
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, ILBTrade> trades = builder.stream(conf.getString(TradeAggregatorConfig.TRADES_TOPIC));
		trades.groupByKey()
			.windowedBy(TimeWindows.of(conf.getAggregationPeriod()))
			.aggregate(LBCandleMutable::new, new LBTradeAggregator(),
				Materialized.<String, LBCandleMutable, WindowStore<Bytes, byte[]>>as(conf.getStoreName())
					.withValueSerde(CaelumSerdes.LBCandleMutable()));
		
		Topology topology = builder.build();
		logger.info("Topology: {}", topology.describe());
		KafkaStreams streams = new KafkaStreams(topology, conf.getKafkaProperties());
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				streams.close();
				logger.info("Finished");
			}
		});
		streams.start();
		logger.info("Started");
	}

}
