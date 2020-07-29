package ru.prolib.caelum.aggregator.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.aggregator.IAggregator;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorServiceBuilder implements IAggregatorServiceBuilder {
	private final KafkaAggregatorBuilder builder;
	
	public KafkaAggregatorServiceBuilder(KafkaAggregatorBuilder builder) {
		this.builder = builder;
	}
	
	public KafkaAggregatorServiceBuilder() {
		this(new KafkaAggregatorBuilder());
	}
	
	protected Periods createPeriods() {
		return new Periods();
	}
	
	protected KafkaUtils createUtils() {
		return KafkaUtils.getInstance();
	}

	protected KafkaAggregatorConfig createConfig(Periods periods) {
		return new KafkaAggregatorConfig(periods);
	}
	
	protected KafkaAggregatorTopologyBuilder createTopologyBuilder() {
		return new KafkaAggregatorTopologyBuilder();
	}
	
	protected KafkaStreamsRegistry createStreamsRegistry(Periods periods) {
		return new KafkaStreamsRegistry(periods);
	}
	
	protected Lock createLock() {
		return new ReentrantLock();
	}

	@Override
	public IAggregatorService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		final Periods periods = createPeriods();
		KafkaAggregatorConfig config = createConfig(periods);
		config.load(default_config_file, config_file);
		KafkaStreamsRegistry streams_registry = createStreamsRegistry(periods);
		Set<String> aggregation_period_list = new LinkedHashSet<>(Arrays.asList(StringUtils.splitByWholeSeparator(
				config.getString(KafkaAggregatorConfig.AGGREGATION_PERIOD), ","))
			.stream()
			.map(String::trim)
			.collect(Collectors.toList()));
		builder.withServices(services)
			.withStreamsRegistry(streams_registry)
			.withTopologyBuilder(createTopologyBuilder())
			.withCleanUpMutex(createLock())
			.withUtils(createUtils());
		List<IAggregator> aggregator_list = new ArrayList<>();
		for ( String aggregation_period : aggregation_period_list ) {
			KafkaAggregatorConfig aggregator_config = createConfig(periods);
			aggregator_config.getProperties().putAll(config.getProperties());
			aggregator_config.getProperties().put(KafkaAggregatorConfig.AGGREGATION_PERIOD, aggregation_period);
			aggregator_list.add(builder.withConfig(aggregator_config).build());
		}
		KafkaAggregatorService service = new KafkaAggregatorService(
				periods,
				streams_registry,
				aggregator_list,
				config.getListTuplesLimit(),
				false);
		return service;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(59710737, 15)
				.append(builder)
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
				.append(o.builder, builder)
				.build();
	}

}
