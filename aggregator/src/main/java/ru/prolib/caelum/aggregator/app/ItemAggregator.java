package ru.prolib.caelum.aggregator.app;

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

import ru.prolib.caelum.aggregator.IKafkaStreamsRegistry;
import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.aggregator.KafkaStreamsRegistryStub;
import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class ItemAggregator {
	static final Logger logger = LoggerFactory.getLogger(ItemAggregator.class);

	public static void main(String[] args) throws Exception {
		ItemAggregator service = new ItemAggregator();
		service.start(args.length > 0 ? args[0] : null);
	}
	
	public void start(ItemAggregatorConfig conf, IKafkaStreamsRegistry registry) throws IOException {
		final String period_code = conf.getOneOfList(ItemAggregatorConfig.AGGREGATION_PERIOD,
				Periods.getInstance().getIntradayPeriodCodes());
		final String store_name = conf.getStoreName();
		final String msg_subj = "item aggregator for";
		logger.info("Starting up {} {}", msg_subj, period_code);
		conf.print(logger);
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, Item> items = builder.stream(conf.getString(ItemAggregatorConfig.SOURCE_TOPIC));
		items.groupByKey()
			.windowedBy(TimeWindows.of(conf.getAggregationPeriod()))
			.aggregate(Tuple::new, new ru.prolib.caelum.aggregator.ItemAggregator(),
				Materialized.<String, Tuple, WindowStore<Bytes, byte[]>>as(store_name)
					.withValueSerde(CaelumSerdes.tupleSerde()))
			.toStream()
			.to(conf.getTargetTopic(), Produced.<Windowed<String>, Tuple>with(
					WindowedSerdes.timeWindowedSerdeFrom(String.class), CaelumSerdes.tupleSerde()));
		
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
				registry.registerAggregator(Period.valueOf(period_code), store_name, streams);
			}
		});
		streams.cleanUp();
		streams.start();
		logger.info("Started {} {}", msg_subj, period_code);
	}
	
	public void start(String conf_file, IKafkaStreamsRegistry registry) throws IOException {
		ItemAggregatorConfig conf = new ItemAggregatorConfig();
		conf.load(ItemAggregatorConfig.DEFAULT_CONFIG_FILE, conf_file);
		start(conf, registry);
	}
	
	public void start(String conf_file) throws IOException {
		start(conf_file, new KafkaStreamsRegistryStub());
	}

}
