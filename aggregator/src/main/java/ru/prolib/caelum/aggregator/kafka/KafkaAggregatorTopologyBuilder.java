package ru.prolib.caelum.aggregator.kafka;

import org.apache.kafka.common.utils.Bytes;
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

import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class KafkaAggregatorTopologyBuilder {
	private static final Logger logger = LoggerFactory.getLogger(KafkaAggregatorTopologyBuilder.class);

	public Topology buildTopology(KafkaAggregatorConfig config) {
		final String target_topic = config.getTargetTopic();
		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, KafkaItem> items = builder.stream(config.getSourceTopic());
		KTable<Windowed<String>, KafkaTuple> table = items.groupByKey()
			.windowedBy(TimeWindows.of(config.getAggregationPeriodDuration()))
			.aggregate(KafkaTuple::new, new KafkaItemAggregator(),
				Materialized.<String, KafkaTuple, WindowStore<Bytes, byte[]>>as(config.getStoreName())
					.withValueSerde(KafkaTupleSerdes.tupleSerde()));

		if ( target_topic != null ) {
			table.toStream().to(target_topic, Produced.<Windowed<String>, KafkaTuple>with(
					WindowedSerdes.timeWindowedSerdeFrom(String.class), KafkaTupleSerdes.tupleSerde()));
			logger.debug("Data aggregated by {} will be stored in topic: {}",
					config.getAggregationPeriodCode(), target_topic);
		}
		Topology topology = builder.build();
		logger.debug("Created topology of item aggregator by {}: {}",
				config.getAggregationPeriod(), topology.describe());
		config.print(logger);
		return topology;
	}

}