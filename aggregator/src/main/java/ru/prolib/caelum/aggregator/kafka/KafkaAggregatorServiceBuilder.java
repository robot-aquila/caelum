package ru.prolib.caelum.aggregator.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.Topology;

import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.core.CompositeService;

public class KafkaAggregatorServiceBuilder implements IAggregatorServiceBuilder {
	private final KafkaAggregatorStreamBuilder streamBuilder;
	
	public KafkaAggregatorServiceBuilder(KafkaAggregatorStreamBuilder streamBuilder) {
		this.streamBuilder = streamBuilder;
	}
	
	public KafkaAggregatorServiceBuilder() {
		this(new KafkaAggregatorStreamBuilder());
	}

	protected KafkaAggregatorConfig createConfig() {
		return new KafkaAggregatorConfig();
	}

	@Override
	public IAggregatorService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		KafkaAggregatorConfig config = createConfig();
		config.load(default_config_file, config_file);
		KafkaAggregatorService service = new KafkaAggregatorService(config.getListTuplesLimit());
		KafkaAggregatorRegistry registry = service.getRegistry();
		Set<String> aggregation_period_list = new LinkedHashSet<>(Arrays.asList(StringUtils.splitByWholeSeparator(
				config.getString(KafkaAggregatorConfig.AGGREGATION_PERIOD), ","))
			.stream()
			.map(String::trim)
			.collect(Collectors.toList()));
		for ( String aggregation_period : aggregation_period_list ) {
			KafkaAggregatorConfig aggregator_config = createConfig();
			aggregator_config.getProperties().putAll(config.getProperties());
			aggregator_config.getProperties().put(KafkaAggregatorConfig.AGGREGATION_PERIOD, aggregation_period);
			Topology topology = streamBuilder.createItemAggregatorTopology(aggregator_config);
			services.register(streamBuilder.buildItemAggregatorStreamsService(registry, topology, aggregator_config));
		}
		return service;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(59710737, 15)
				.append(streamBuilder)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorServiceBuilder.class ) {
			return false;
		}
		KafkaAggregatorServiceBuilder o = (KafkaAggregatorServiceBuilder) other;
		return new EqualsBuilder()
				.append(o.streamBuilder, streamBuilder)
				.build();
	}

}
