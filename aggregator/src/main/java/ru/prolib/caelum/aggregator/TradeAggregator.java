package ru.prolib.caelum.aggregator;

import java.io.IOException;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.IKafkaStreamsRegistry;
import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.KafkaStreamsRegistryStub;
import ru.prolib.caelum.core.LBOHLCVMutable;
import ru.prolib.caelum.core.LBTradeAggregator;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class TradeAggregator {
	static final Logger logger = LoggerFactory.getLogger(TradeAggregator.class);

	public static void main(String[] args) throws Exception {
		TradeAggregator service = new TradeAggregator();
		service.start(args.length > 0 ? args[0] : null);
	}
	
	public void start(TradeAggregatorConfig conf, IKafkaStreamsRegistry registry) throws IOException {
		final String period_code = conf.getOneOfList(TradeAggregatorConfig.AGGREGATION_PERIOD,
				Periods.getInstance().getIntradayPeriodCodes());
		final String store_name = conf.getStoreName();
		final String msg_subj = "trade to OHLCV aggregator for";
		logger.info("Starting up {} {}", msg_subj, period_code);
		conf.print(logger);
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, ILBTrade> trades = builder.stream(conf.getString(TradeAggregatorConfig.SOURCE_TOPIC));
		trades.groupByKey()
			.windowedBy(TimeWindows.of(conf.getAggregationPeriod()))
			.aggregate(LBOHLCVMutable::new, new LBTradeAggregator(),
				Materialized.<String, LBOHLCVMutable, WindowStore<Bytes, byte[]>>as(store_name)
					.withValueSerde(CaelumSerdes.LBOHLCVMutable()))
			.toStream()
			.to(conf.getTargetTopic(), Produced.<Windowed<String>, LBOHLCVMutable>with(
					WindowedSerdes.timeWindowedSerdeFrom(String.class), CaelumSerdes.LBOHLCVMutable()));
		
		Topology topology = builder.build();
		logger.info("Topology: {}", topology.describe());
		KafkaStreams streams = new KafkaStreams(topology, conf.getKafkaProperties());
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				streams.close();
				logger.info("Finished {} {}", msg_subj, period_code);
			}
		});
		streams.setStateListener((new_state, old_state) -> {
			if ( new_state == KafkaStreams.State.RUNNING ) {
				registry.registerOHLCVAggregator(Period.valueOf(period_code), store_name, streams);
			}
		});
		streams.cleanUp();
		streams.start();
		logger.info("Started {} {}", msg_subj, period_code);
	}
	
	public void start(String conf_file, IKafkaStreamsRegistry registry) throws IOException {
		TradeAggregatorConfig conf = new TradeAggregatorConfig();
		conf.load(TradeAggregatorConfig.DEFAULT_CONFIG_FILE, conf_file);
		start(conf, registry);
	}
	
	public void start(String conf_file) throws IOException {
		start(conf_file, new KafkaStreamsRegistryStub());
	}

}
