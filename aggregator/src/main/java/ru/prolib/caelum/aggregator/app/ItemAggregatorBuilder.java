package ru.prolib.caelum.aggregator.app;

import java.io.IOException;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.AggregatorType;
import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.aggregator.kafka.AggregatorDescr;
import ru.prolib.caelum.aggregator.kafka.AggregatorService;
import ru.prolib.caelum.aggregator.kafka.KafkaTuple;
import ru.prolib.caelum.aggregator.kafka.KafkaTupleSerdes;
import ru.prolib.caelum.aggregator.kafka.utils.KafkaStreamsService;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class ItemAggregatorBuilder {
	private static final Logger logger = LoggerFactory.getLogger(ItemAggregatorBuilder.class);
	private final AggregatorService aggregatorService;
	
	public ItemAggregatorBuilder(AggregatorService aggregator_service) {
		this.aggregatorService = aggregator_service;
	}
	
	public ItemAggregatorBuilder() {
		this(new AggregatorService());
	}
	
	public AggregatorService getAggregatorService() {
		return aggregatorService;
	}
	
	public IService build(ItemAggregatorConfig conf) {
		final String period_code = conf.getAggregationPeriodCode();
		final String store_name = conf.getStoreName();
		final String source_topic = conf.getString(ItemAggregatorConfig.SOURCE_TOPIC);
		final String target_topic = conf.getTargetTopic();
		
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, KafkaItem> items = builder.stream(source_topic);
		KTable<Windowed<String>, KafkaTuple> table = items.groupByKey()
			.windowedBy(TimeWindows.of(conf.getAggregationPeriod()))
			.aggregate(KafkaTuple::new, new ru.prolib.caelum.aggregator.kafka.KafkaItemAggregator(),
				Materialized.<String, KafkaTuple, WindowStore<Bytes, byte[]>>as(store_name)
					.withValueSerde(KafkaTupleSerdes.tupleSerde()));

		if ( target_topic != null ) {
			table.toStream().to(conf.getTargetTopic(), Produced.<Windowed<String>, KafkaTuple>with(
					WindowedSerdes.timeWindowedSerdeFrom(String.class), KafkaTupleSerdes.tupleSerde()));
			logger.debug("Data aggregated by {} will be stored in topic: {}", period_code, target_topic);
		}
		Topology topology = builder.build();
		logger.debug("Topology of item aggregator by {}: {}", period_code, topology.describe());
		KafkaStreams streams = new KafkaStreams(topology, conf.getKafkaProperties());
		streams.setStateListener((new_state, old_state) -> {
			if ( new_state == KafkaStreams.State.RUNNING ) {
				aggregatorService.register(new AggregatorDescr(AggregatorType.ITEM,
					Period.valueOf(period_code), source_topic, target_topic, store_name), streams);
			}
		});
		return new KafkaStreamsService(streams, "Item aggregator by " + period_code, conf);
	}
	
	public IService build(String conf_file) throws IOException {
		ItemAggregatorConfig conf = new ItemAggregatorConfig();
		conf.load(ItemAggregatorConfig.DEFAULT_CONFIG_FILE, conf_file);
		return build(conf);
	}

}
