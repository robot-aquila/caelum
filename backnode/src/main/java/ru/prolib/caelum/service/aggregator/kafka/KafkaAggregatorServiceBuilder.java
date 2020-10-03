package ru.prolib.caelum.service.aggregator.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
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
	private final KafkaUtils utils;
	
	public KafkaAggregatorServiceBuilder(KafkaAggregatorBuilder builder, KafkaUtils utils) {
		this.builder = builder;
		this.utils = utils;
	}
	
	public KafkaAggregatorServiceBuilder() {
		this(new KafkaAggregatorBuilder(), KafkaUtils.getInstance());
	}
	
	public KafkaAggregatorBuilder getKafkaAggregatorBuilder() {
		return builder;
	}
	
	public KafkaUtils getKafkaUtils() {
		return utils;
	}
	
	protected KafkaStreamsRegistry createStreamsRegistry(HostInfo hostInfo, Intervals intervals) {
		return new KafkaStreamsRegistry(hostInfo, intervals);
	}
	
	protected Lock createLock() {
		return new ReentrantLock();
	}
	
	/**
	 * Create initialization service.
	 * <p>
	 * @param config - configuration
	 * @return 
	 */
	protected IService createInitService(GeneralConfig config) {
		return new KafkaCreateTopicService(utils,
				config,
				new NewTopic(config.getItemsTopicName(),
						config.getItemsTopicNumPartitions(),
						config.getItemsTopicReplicationFactor()
					).configs(toMap("retention.ms", Long.toString(config.getItemsTopicRetentionTime()))),
				config.getDefaultTimeout());
	}
	
	private boolean isParallelClear(GeneralConfig config) {
		Boolean force = config.getAggregatorKafkaForceParallelClear();
		if ( force != null ) return force;
		return utils.isOsUnix();
	}

	@Override
	public IAggregatorService build(IBuildingContext context) throws IOException {
		GeneralConfig config = context.getConfig();
		context.registerService(createInitService(config));
		boolean isParallelClear = isParallelClear(config);
		logger.debug("isParallelClear: {}", isParallelClear);
		Intervals intervals = config.getIntervals();
		
		KafkaStreamsRegistry streamsRegistry = createStreamsRegistry(config.getHttpInfo(), intervals);
		builder.withBuildingContext(context)
			.withStreamsRegistry(streamsRegistry)
			.withTopologyBuilder(new KafkaAggregatorTopologyBuilder())
			.withCleanUpMutex(createLock())
			.withUtils(utils);
		
		 List<IAggregator> aggrList =
			Arrays.asList(StringUtils.splitByWholeSeparator(config.getAggregatorInterval(), ","))
			.stream()
			.map(String::trim) // remove possible spaces
			.collect(Collectors.toSet()) // remove possible duplicates
			.stream()
			.map((c) -> builder.withConfig(new KafkaAggregatorConfig(intervals.getIntervalByCode(c), config)).build())
			.collect(Collectors.toList());
		return new KafkaAggregatorService(intervals, streamsRegistry, aggrList, config.getMaxTuplesLimit(),
				isParallelClear, config.getDefaultTimeout());
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
