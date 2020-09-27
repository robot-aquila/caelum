package ru.prolib.caelum.service.aggregator.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.IAggregatorService;
import ru.prolib.caelum.service.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaCreateTopicService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorServiceBuilder implements IAggregatorServiceBuilder {
	private static final Logger logger = LoggerFactory.getLogger(KafkaAggregatorServiceBuilder.class);
	
	public static Map<String, String> toMap(String ...args) {
		Map<String, String> result = new LinkedHashMap<>();
		int count = args.length / 2;
		if ( args.length % 2 != 0 ) throw new IllegalArgumentException();
		for ( int i = 0; i < count; i ++ ) {
			result.put(args[i * 2], args[i * 2 + 1]);
		}
		return result;
	}
	
	private final KafkaAggregatorBuilder builder;
	
	public KafkaAggregatorServiceBuilder(KafkaAggregatorBuilder builder) {
		this.builder = builder;
	}
	
	public KafkaAggregatorServiceBuilder() {
		this(new KafkaAggregatorBuilder());
	}
	
	protected Intervals createIntervals() {
		return new Intervals();
	}
	
	protected KafkaUtils createUtils() {
		return KafkaUtils.getInstance();
	}

	protected KafkaAggregatorConfig createConfig(Intervals intervals) {
		return new KafkaAggregatorConfig(intervals);
	}
	
	protected KafkaAggregatorTopologyBuilder createTopologyBuilder() {
		return new KafkaAggregatorTopologyBuilder();
	}
	
	protected KafkaStreamsRegistry createStreamsRegistry(HostInfo hostInfo, Intervals intervals) {
		return new KafkaStreamsRegistry(hostInfo, intervals);
	}
	
	protected Lock createLock() {
		return new ReentrantLock();
	}

	@Override
	public IAggregatorService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		final Intervals intervals = createIntervals();
		KafkaAggregatorConfig config = createConfig(intervals);
		final KafkaUtils utils = createUtils();
		config.load(default_config_file, config_file);
		services.register(new KafkaCreateTopicService(utils,
				config.getAdminClientProperties(),
				new NewTopic(config.getSourceTopic(),
						config.getSourceTopicNumPartitions(),
						config.getSourceTopicReplicationFactor()
					).configs(toMap("retention.ms", Long.toString(config.getSourceTopicRetentionTime()))),
				config.getDefaultTimeout()));
		boolean is_parallel_clear = config.isParallelClear();
		logger.debug("isParallelClear: {}", is_parallel_clear);
		
		KafkaStreamsRegistry streams_registry = createStreamsRegistry(config.getApplicationServer(), intervals);
		Set<String> aggregation_interval_list = new LinkedHashSet<>(Arrays.asList(StringUtils.splitByWholeSeparator(
				config.getString(KafkaAggregatorConfig.INTERVAL), ","))
			.stream()
			.map(String::trim)
			.collect(Collectors.toList()));
		builder.withServices(services)
			.withStreamsRegistry(streams_registry)
			.withTopologyBuilder(createTopologyBuilder())
			.withCleanUpMutex(createLock())
			.withUtils(utils);
		List<IAggregator> aggregator_list = new ArrayList<>();
		for ( String aggregation_interval : aggregation_interval_list ) {
			KafkaAggregatorConfig aggregator_config = createConfig(intervals);
			aggregator_config.getProperties().putAll(config.getProperties());
			aggregator_config.getProperties().put(KafkaAggregatorConfig.INTERVAL, aggregation_interval);
			aggregator_list.add(builder.withConfig(aggregator_config).build());
		}
		KafkaAggregatorService service = new KafkaAggregatorService(
				intervals,
				streams_registry,
				aggregator_list,
				config.getListTuplesLimit(),
				is_parallel_clear,
				config.getDefaultTimeout());
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
