package ru.prolib.caelum.service.aggregator.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;
import ru.prolib.caelum.service.aggregator.AggregatorConfig;

public class KafkaAggregatorConfig extends AggregatorConfig {
	public static final String APPLICATION_ID_PREFIX		= "caelum.aggregator.kafka.pfx.application.id";
	public static final String AGGREGATION_STORE_PREFIX		= "caelum.aggregator.kafka.pfx.aggregation.store";
	public static final String TARGET_TOPIC_PREFIX			= "caelum.aggregator.kafka.pfx.target.topic";
	public static final String BOOTSTRAP_SERVERS			= "caelum.aggregator.kafka.bootstrap.servers";
	public static final String SOURCE_TOPIC					= "caelum.aggregator.kafka.source.topic";
	public static final String SOURCE_TOPIC_RETENTION_TIME	= "caelum.aggregator.kafka.source.topic.retention.time";
	public static final String SOURCE_TOPIC_NUM_PARTITIONS	= "caelum.aggregator.kafka.source.topic.num.partitions";
	public static final String SOURCE_TOPIC_REPLIC_FACTOR	= "caelum.aggregator.kafka.source.topic.replication.factor";
	public static final String MAX_ERRORS					= "caelum.aggregator.kafka.max.errors";
	public static final String DEFAULT_TIMEOUT				= "caelum.aggregator.kafka.default.timeout";
	public static final String FORCE_PARALLEL_CLEAR			= "caelum.aggregator.kafka.force.parallel.clear";
	public static final String LINGER_MS					= "caelum.aggregator.kafka.linger.ms";
	public static final String STATE_DIR					= "caelum.aggregator.kafka.state.dir";
	public static final String NUM_STREAM_THREADS			= "caelum.aggregator.kafka.num.stream.threads";
	public static final String STORE_RETENTION_TIME			= "caelum.aggregator.kafka.store.retention.time";
	public static final String APPLICATION_SERVER			= "caelum.aggregator.kafka.application.server";

	private final Intervals intervals;
	
	public KafkaAggregatorConfig(Intervals interval) {
		super();
		this.intervals = interval;
	}
	
	public Intervals getIntervals() {
		return intervals;
	}
	
	@Override
	public void setDefaults() {
		super.setDefaults();
		props.put(APPLICATION_ID_PREFIX, "caelum-item-aggregator-");
		props.put(AGGREGATION_STORE_PREFIX, "caelum-tuple-store-");
		props.put(TARGET_TOPIC_PREFIX, "caelum-tuple-");
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(SOURCE_TOPIC, "caelum-item");
		props.put(SOURCE_TOPIC_REPLIC_FACTOR, "1");
		props.put(SOURCE_TOPIC_NUM_PARTITIONS, "8");
		props.put(SOURCE_TOPIC_RETENTION_TIME, "31536000000000");
		props.put(MAX_ERRORS, "99");
		props.put(DEFAULT_TIMEOUT, "60000");
		props.put(FORCE_PARALLEL_CLEAR, "");
		props.put(LINGER_MS, "5");
		props.put(STATE_DIR, "/tmp/kafka-streams");
		props.put(NUM_STREAM_THREADS, "2");
		props.put(STORE_RETENTION_TIME, "31536000000000");
		props.put(APPLICATION_SERVER, "localhost:9698");
	}
	
	private String getSuffix() {
		return getString(INTERVAL).toLowerCase();
	}

	public String getApplicationId() {
		return getString(APPLICATION_ID_PREFIX) + getSuffix();
	}

	public String getStoreName() {
		return getString(AGGREGATION_STORE_PREFIX) + getSuffix();
	}
	
	public long getStoreRetentionTime() {
		return getLong(STORE_RETENTION_TIME);
	}
	
	public String getAggregationIntervalCode() {
		return getOneOfList(KafkaAggregatorConfig.INTERVAL, intervals.getIntervalCodes());
	}
	
	public Interval getAggregationInterval() {
		return intervals.getIntervalByCode(getAggregationIntervalCode());
	}
	
	public Duration getAggregationIntervalDuration() {
		return intervals.getIntervalDurationByCode(getAggregationIntervalCode());
	}
	
	/**
	 * Get topic to store aggregated data.
	 * <p>
	 * @return topic or null if topic is not defined and data shouldn't be stored
	 */
	public String getTargetTopic() {
		String prefix = getString(TARGET_TOPIC_PREFIX);
		return "".equals(prefix) ? null : prefix + getSuffix();
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}
	
	public int getSourceTopicNumPartitions() {
		return getInt(SOURCE_TOPIC_NUM_PARTITIONS);
	}
	
	public short getSourceTopicReplicationFactor() {
		return getShort(SOURCE_TOPIC_REPLIC_FACTOR);
	}
	
	public long getSourceTopicRetentionTime() {
		return getLong(SOURCE_TOPIC_RETENTION_TIME);
	}
	
	public int getMaxErrors() {
		return getInt(MAX_ERRORS);
	}
	
	public long getDefaultTimeout() {
		return getInt(DEFAULT_TIMEOUT);
	}
	
	protected boolean isOsUnix() {
		return SystemUtils.IS_OS_UNIX;
	}
	
	public boolean isParallelClear() {
		return getBoolean(FORCE_PARALLEL_CLEAR, isOsUnix());
	}
	
	public HostInfo getApplicationServer() {
		String chunks[] = StringUtils.split(getString(APPLICATION_SERVER), ':');
		return new HostInfo(chunks[0], chunks.length == 1 ? 9698 : Integer.parseInt(chunks[1]));
	}
	
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getString(BOOTSTRAP_SERVERS));
		conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KafkaItemSerdes.keySerde().getClass());
		conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaItemSerdes.itemSerde().getClass());
		conf.put(StreamsConfig.STATE_DIR_CONFIG, getString(STATE_DIR));
		conf.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, getString(NUM_STREAM_THREADS));
		conf.put(StreamsConfig.APPLICATION_SERVER_CONFIG, getString(APPLICATION_SERVER));
		conf.put(ProducerConfig.LINGER_MS_CONFIG, getString(LINGER_MS));
		
		conf.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
		//conf.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "500");
		//conf.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
		//conf.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
		return conf;
	}
	
	public Properties getAdminClientProperties() {
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		return conf;
	}

}
