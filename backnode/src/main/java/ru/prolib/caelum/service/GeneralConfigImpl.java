package ru.prolib.caelum.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;

import ru.prolib.caelum.backnode.RestServiceBuilder;
import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorServiceBuilder;
import ru.prolib.caelum.service.itemdb.kafka.KafkaItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.itesym.ItesymBuilder;
import ru.prolib.caelum.service.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.service.symboldb.fdb.FDBSymbolServiceBuilder;

public class GeneralConfigImpl extends GeneralConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.caelum-backnode.properties";
	private static final String
		EXTENSION_BUILDER_PFX	= "caelum.extension.builder.",
		
		MODE									= "caelum.backnode.mode",
		HTTP_HOST								= "caelum.backnode.http.host",
		HTTP_PORT								= "caelum.backnode.http.port",
		ADVERTISED_HTTP_HOST					= "caelum.backnode.adv.http.host",
		ADVERTISED_HTTP_PORT					= "caelum.backnode.adv.http.port",
		MAX_ERRORS								= "caelum.max.errors",
		DEFAULT_TIMEOUT							= "caelum.default.timeout",
		SHUTDOWN_TIMEOUT						= "caelum.shutdown.timeout",
		MAX_ITEMS_LIMIT							= "caelum.max.items.limit",
		MAX_SYMBOLS_LIMIT						= "caelum.max.symbols.limit",
		MAX_EVENTS_LIMIT						= "caelum.max.events.limit",
		MAX_TUPLES_LIMIT						= "caelum.max.tuples.limit",
		KAFKA_BOOTSTRAP_SERVERS					= "caelum.kafka.bootstrap.servers",
		KAFKA_STATE_DIR							= "caelum.kafka.state.dir",
		KAFKA_POLL_TIMEOUT						= "caelum.kafka.poll.timeout",
		KAFKA_TOPIC_ITEMS_NAME					= "caelum.kafka.topic.items.name",
		KAFKA_TOPIC_ITEMS_RETENTION_TIME		= "caelum.kafka.topic.items.retention.time",
		KAFKA_TOPIC_ITEMS_NUM_PARTITIONS		= "caelum.kafka.topic.items.num.partitions",
		KAFKA_TOPIC_ITEMS_REPLICATION_FACTOR	= "caelum.kafka.topic.items.replication.factor",
		FDB_CLUSTER								= "caelum.fdb.cluster",
		FDB_SUBSPACE							= "caelum.fdb.subspace",
		ITEMDB_SERVICE_BUILDER					= "caelum.itemdb.builder",
		ITEMDB_SERVICE_KAFKA_TRANSACTIONAL_ID	= "caelum.itemdb.kafka.transactional.id",
		SYMBOLDB_SERVICE_BUILDER				= "caelum.symboldb.builder",
		SYMBOLDB_SERVICE_CATEGORY_EXTRACTOR		= "caelum.symboldb.category.extractor",
		AGGREGATOR_SERVICE_BUILDER				= "caelum.aggregator.builder",
		AGGREGATOR_INTERVAL						= "caelum.aggregator.interval",
		AGGREGATOR_KAFKA_APPLICATION_ID_PFX		= "caelum.aggregator.kafka.application.id.pfx",
		AGGREGATOR_KAFKA_STORE_PFX				= "caelum.aggregator.kafka.store.pfx",
		AGGREGATOR_KAFKA_STORE_RETENTION_TIME	= "caelum.aggregator.kafka.store.retention.time",
		AGGREGATOR_KAFKA_TARGET_TOPIC_PFX		= "caelum.aggregator.kafka.target.topic.pfx",
		AGGREGATOR_KAFKA_FORCE_PARALLEL_CLEAR	= "caelum.aggregator.kafka.force.parallel.clear",
		AGGREGATOR_KAFKA_LINGER_MS				= "caelum.aggregator.kafka.linger.ms",
		AGGREGATOR_KAFKA_NUM_STREAM_THREADS		= "caelum.aggregator.kafka.num.stream.threads",
		
		ITESYM_EXTENSION						= "caelum.extension.builder.Itesym",
		ITESYM_KAFKA_GROUP_ID					= "caelum.itesym.kafka.group.id",
		REST_EXTENSION							= "caelum.extension.builder.REST",
		HTTP_EXTENSION							= "caelum.extension.builder.HTTP";

	private final Intervals intervals;
	
	public GeneralConfigImpl(Intervals intervals) {
		this.intervals = intervals;
	}
	
	public GeneralConfigImpl() {
		this(new Intervals());
	}
	
	protected String randomUUID() {
		return UUID.randomUUID().toString();
	}
	
	@Override
	public Intervals getIntervals() {
		return intervals;
	}

	@Override
	protected String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
	@Override
	protected void setDefaults() {
		props.put(MODE, "prod");
		props.put(HTTP_HOST, "localhost");
		props.put(HTTP_PORT, "9698");
		props.put(ADVERTISED_HTTP_HOST, "localhost");
		props.put(ADVERTISED_HTTP_PORT, "9698");
		props.put(MAX_ERRORS, "99");
		props.put(DEFAULT_TIMEOUT, "60000");
		props.put(SHUTDOWN_TIMEOUT, "15000");
		props.put(MAX_ITEMS_LIMIT, "5000");
		props.put(MAX_SYMBOLS_LIMIT, "5000");
		props.put(MAX_EVENTS_LIMIT, "5000");
		props.put(MAX_TUPLES_LIMIT, "5000");
		props.put(KAFKA_BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(KAFKA_STATE_DIR, "/tmp/kafka-streams");
		props.put(KAFKA_POLL_TIMEOUT, "1000");
		props.put(KAFKA_TOPIC_ITEMS_NAME, "caelum-item");
		props.put(KAFKA_TOPIC_ITEMS_RETENTION_TIME, "31536000000000");
		props.put(KAFKA_TOPIC_ITEMS_NUM_PARTITIONS, "32");
		props.put(KAFKA_TOPIC_ITEMS_REPLICATION_FACTOR, "1");
		props.put(FDB_CLUSTER, "");
		props.put(FDB_SUBSPACE, "caelum");
		
		props.put(ITEMDB_SERVICE_BUILDER, KafkaItemDatabaseServiceBuilder.class.getName());
		props.put(SYMBOLDB_SERVICE_BUILDER, FDBSymbolServiceBuilder.class.getName());
		props.put(AGGREGATOR_SERVICE_BUILDER, KafkaAggregatorServiceBuilder.class.getName());
		
		props.put(ITEMDB_SERVICE_KAFKA_TRANSACTIONAL_ID, "");
		props.put(SYMBOLDB_SERVICE_CATEGORY_EXTRACTOR, CommonCategoryExtractor.class.getName());
		props.put(AGGREGATOR_INTERVAL, "M1,H1");
		props.put(AGGREGATOR_KAFKA_APPLICATION_ID_PFX, "caelum-item-aggregator-");
		props.put(AGGREGATOR_KAFKA_STORE_PFX, "caelum-tuple-store-");
		props.put(AGGREGATOR_KAFKA_STORE_RETENTION_TIME, "31536000000000");
		props.put(AGGREGATOR_KAFKA_TARGET_TOPIC_PFX, "caelum-tuple-");
		props.put(AGGREGATOR_KAFKA_FORCE_PARALLEL_CLEAR, "");
		props.put(AGGREGATOR_KAFKA_LINGER_MS, "5");
		props.put(AGGREGATOR_KAFKA_NUM_STREAM_THREADS, "2");
		
		props.put(ITESYM_EXTENSION, "on::" + ItesymBuilder.class.getName());
		props.put(ITESYM_KAFKA_GROUP_ID, "caelum-itesym");
		props.put(REST_EXTENSION, "on::" + RestServiceBuilder.class.getName());
		props.put(HTTP_EXTENSION, "on:last:" + JettyServerBuilder.class.getName());
	}
	
	@Override
	public Mode getMode() {
		return Mode.valueOf(getOneOfList("caelum.backnode.mode", Arrays.asList("test", "prod")).toUpperCase());
	}
	
	public GeneralConfigImpl setMode(Mode mode) {
		props.put(MODE, mode.toString().toLowerCase());
		return this;
	}
	
	@Override
	public String getHttpHost() {
		return getString(HTTP_HOST);
	}
	
	@Override
	public int getHttpPort() {
		return getInt(HTTP_PORT);
	}
	
	@Override
	public HostInfo getHttpInfo() {
		return new HostInfo(getHttpHost(), getHttpPort());
	}
	
	@Override
	public HostInfo getAdvertisedHttpInfo() {
		return new HostInfo(getString(ADVERTISED_HTTP_HOST), getInt(ADVERTISED_HTTP_PORT));
	}
	
	public GeneralConfigImpl setHttpInfo(String host, int port) {
		props.put(HTTP_HOST, host);
		props.put(HTTP_PORT, Integer.toString(port));
		return this;
	}
	
	public GeneralConfigImpl setAdvertisedHttpInfo(String host, int port) {
		props.put(ADVERTISED_HTTP_HOST, host);
		props.put(ADVERTISED_HTTP_PORT, Integer.toString(port));
		return this;
	}
	
	@Override
	public short getItemsTopicReplicationFactor() {
		return getShort(KAFKA_TOPIC_ITEMS_REPLICATION_FACTOR);
	}
	
	public GeneralConfigImpl setItemsTopicReplicationFactor(short factor) {
		props.put(KAFKA_TOPIC_ITEMS_REPLICATION_FACTOR, Short.toString(factor));
		return this;
	}
	
	@Override
	public int getItemsTopicNumPartitions() {
		return getInt(KAFKA_TOPIC_ITEMS_NUM_PARTITIONS);
	}
	
	public GeneralConfigImpl setItemsTopicNumPartitions(int num) {
		props.put(KAFKA_TOPIC_ITEMS_NUM_PARTITIONS, Integer.toString(num));
		return this;
	}
	
	@Override
	public long getItemsTopicRetentionTime() {
		return getLong(KAFKA_TOPIC_ITEMS_RETENTION_TIME);
	}
	
	public GeneralConfigImpl setItemsTopicRetentionTime(long time) {
		props.put(KAFKA_TOPIC_ITEMS_RETENTION_TIME, Long.toString(time));
		return this;
	}
	
	@Override
	public String getItemsTopicName() {
		return getString(KAFKA_TOPIC_ITEMS_NAME);
	}
	
	public GeneralConfigImpl setItemsTopicName(String name) {
		props.put(KAFKA_TOPIC_ITEMS_NAME, name);
		return this;
	}
	
	@Override
	public int getMaxErrors() {
		return getInt(MAX_ERRORS);
	}
	
	public GeneralConfigImpl setMaxErrors(int maxErrors) {
		props.put(MAX_ERRORS, Integer.toString(maxErrors));
		return this;
	}
	
	@Override
	public long getDefaultTimeout() {
		return getLong(DEFAULT_TIMEOUT);
	}
	
	public GeneralConfigImpl setDefaultTimeout(long timeout) {
		props.put(DEFAULT_TIMEOUT, Long.toString(timeout));
		return this;
	}
	
	@Override
	public long getShutdownTimeout() {
		return getLong(SHUTDOWN_TIMEOUT);
	}
	
	public GeneralConfigImpl setShutdownTimeout(long timeout) {
		props.put(SHUTDOWN_TIMEOUT, Long.toString(timeout));
		return this;
	}
	
	@Override
	public String getKafkaBootstrapServers() {
		return getString(KAFKA_BOOTSTRAP_SERVERS);
	}
	
	public GeneralConfigImpl setKafkaBootstrapServers(String servers) {
		props.put(KAFKA_BOOTSTRAP_SERVERS, servers);
		return this;
	}
	
	@Override
	public String getKafkaStateDir() {
		return getString(KAFKA_STATE_DIR);
	}
	
	public GeneralConfigImpl setKafkaStateDir(String dir) {
		props.put(KAFKA_STATE_DIR, dir);
		return this;
	}
	
	@Override
	public long getKafkaPollTimeout() {
		return getLong(KAFKA_POLL_TIMEOUT);
	}
	
	public GeneralConfigImpl setKafkaPollTimeout(long timeout) {
		props.put(KAFKA_POLL_TIMEOUT, Long.toString(timeout));
		return this;
	}
	
	@Override
	public String getFdbCluster() {
		return getString(FDB_CLUSTER);
	}
	
	public GeneralConfigImpl setFdbCluster(String cluster) {
		props.put(FDB_CLUSTER, cluster);
		return this;
	}
	
	@Override
	public String getFdbSubspace() {
		return getString(FDB_SUBSPACE);
	}
	
	public GeneralConfigImpl setFdbSubspace(String subspace) {
		props.put(FDB_SUBSPACE, subspace);
		return this;
	}
	
	@Override
	public int getMaxItemsLimit() {
		return getInt(MAX_ITEMS_LIMIT);
	}
	
	public GeneralConfigImpl setMaxItemsLimit(int limit) {
		props.put(MAX_ITEMS_LIMIT, Integer.toString(limit));
		return this;
	}
	
	@Override
	public int getMaxSymbolsLimit() {
		return getInt(MAX_SYMBOLS_LIMIT);
	}
	
	public GeneralConfigImpl setMaxSymbolsLimit(int limit) {
		props.put(MAX_SYMBOLS_LIMIT, Integer.toString(limit));
		return this;
	}
	
	@Override
	public int getMaxEventsLimit() {
		return getInt(MAX_EVENTS_LIMIT);
	}
	
	public GeneralConfigImpl setMaxEventsLimit(int limit) {
		props.put(MAX_EVENTS_LIMIT, Integer.toString(limit));
		return this;
	}
	
	@Override
	public int getMaxTuplesLimit() {
		return getInt(MAX_TUPLES_LIMIT);
	}
	
	public GeneralConfigImpl setMaxTuplesLimit(int limit) {
		props.put(MAX_TUPLES_LIMIT, Integer.toString(limit));
		return this;
	}
	
	public static class ExtConfComparator implements Comparator<ExtensionConf> {

		@Override
		public int compare(ExtensionConf a, ExtensionConf b) {
			StartupOrder aso = a.getStartupOrder(), bso = b.getStartupOrder();
			Integer ao = aso.getOrder() == null ? Integer.MAX_VALUE : aso.getOrder(),
					bo = bso.getOrder() == null ? Integer.MAX_VALUE : bso.getOrder();
			return new CompareToBuilder()
					.append(aso.getPriority().ordinal(), bso.getPriority().ordinal())
					.append(ao, bo)
					.append(a.getId(), b.getId())
					.build();
		}
		
	}
	
	protected boolean parseExtEnabled(String string) {
		switch ( string.toLowerCase() ) {
		case "":
		case "on":
			return true;
		case "off": return false;
		default:
			throw new IllegalArgumentException("Unidentified activity sign: " + string);
		}
	}
	
	protected StartupOrder parseExtStartupOrder(String string) {
		switch ( string.toLowerCase() ) {
		case "first":
			return new StartupOrder(StartupPriority.FIRST, null);
		case "last":
			return new StartupOrder(StartupPriority.LAST, null);
		case "":
			return new StartupOrder(StartupPriority.NORMAL, null);
		default:
			return new StartupOrder(StartupPriority.NORMAL, Integer.parseInt(string));
		}
	}
	
	protected ExtensionConf parseExtConf(String id, String string) {
		String[] chunks = StringUtils.splitPreserveAllTokens(string, ':');
		if ( chunks.length == 1 ) {
			return new ExtensionConf(id, chunks[0], true, parseExtStartupOrder(""));
		} else if ( chunks.length == 2 ) {
			return new ExtensionConf(id, chunks[1], true, parseExtStartupOrder(chunks[0]));
		} else if ( chunks.length == 3 ) {
			return new ExtensionConf(id, chunks[2], parseExtEnabled(chunks[0]), parseExtStartupOrder(chunks[1]));
		} else {
			throw new IllegalArgumentException("Incorrect format of extension builder string: " + string);
		}
	}
	
	public Collection<ExtensionConf> getExtensions() {
		List<ExtensionConf> result = new ArrayList<>();
		for ( String key : props.stringPropertyNames() ) {
			if ( key.startsWith(EXTENSION_BUILDER_PFX) ) {
				String id = key.substring(EXTENSION_BUILDER_PFX.length());
				result.add(parseExtConf(id, props.getProperty(key)));
			}
		}
		Collections.sort(result, new ExtConfComparator());
		return result;
	}
	
	@Override
	public String getItemServiceBuilder() {
		return getString(ITEMDB_SERVICE_BUILDER);
	}
	
	public GeneralConfigImpl setItemServiceBuilder(String builderClass) {
		props.put(ITEMDB_SERVICE_BUILDER, builderClass);
		return this;
	}
	
	@Override
	public String getItemServiceKafkaTransactionalId() {
		String trans_id = props.getProperty(ITEMDB_SERVICE_KAFKA_TRANSACTIONAL_ID);
		return trans_id == null || "".equals(trans_id) ? randomUUID() : trans_id;
	}
	
	@Override
	public String getSymbolServiceBuilder() {
		return getString(SYMBOLDB_SERVICE_BUILDER);
	}
	
	public GeneralConfigImpl setSymbolServiceBuilder(String builderClass) {
		props.put(SYMBOLDB_SERVICE_BUILDER, builderClass);
		return this;
	}
	
	@Override
	public String getSymbolServiceCategoryExtractor() {
		return getString(SYMBOLDB_SERVICE_CATEGORY_EXTRACTOR);
	}
	
	public GeneralConfigImpl setSymbolServiceCategoryExtractor(String extractorClass) {
		props.put(SYMBOLDB_SERVICE_CATEGORY_EXTRACTOR, extractorClass);
		return this;
	}
	
	@Override
	public String getAggregatorServiceBuilder() {
		return getString(AGGREGATOR_SERVICE_BUILDER);
	}
	
	public GeneralConfigImpl setAggregatorServiceBuilder(String builderClass) {
		props.put(AGGREGATOR_SERVICE_BUILDER, builderClass);
		return this;
	}
	
	@Override
	public String getAggregatorInterval() {
		return getString(AGGREGATOR_INTERVAL);
	}
	
	public GeneralConfigImpl setAggregatorInterval(String code) {
		props.put(AGGREGATOR_INTERVAL, code);
		return this;
	}
	
	@Override
	public String getAggregatorKafkaApplicationIdPrefix() {
		return getString(AGGREGATOR_KAFKA_APPLICATION_ID_PFX);
	}
	
	public GeneralConfigImpl setAggregatorKafkaApplicationIdPrefix(String prefix) {
		props.put(AGGREGATOR_KAFKA_APPLICATION_ID_PFX, prefix);
		return this;
	}
	
	@Override
	public String getAggregatorKafkaStorePrefix() {
		return getString(AGGREGATOR_KAFKA_STORE_PFX);
	}
	
	public GeneralConfigImpl setAggregatorKafkaStorePrefix(String prefix) {
		props.put(AGGREGATOR_KAFKA_STORE_PFX, prefix);
		return this;
	}
	
	@Override
	public String getAggregatorKafkaTargetTopicPrefix() {
		return getString(AGGREGATOR_KAFKA_TARGET_TOPIC_PFX);
	}
	
	public GeneralConfigImpl setAggregatorKafkaTargetTopicPrefix(String prefix) {
		props.put(AGGREGATOR_KAFKA_TARGET_TOPIC_PFX, prefix);
		return this;
	}
	
	@Override
	public String getAggregatorKafkaApplicationServer() {
		return getHttpHost() + ":" + getHttpPort();
	}
	
	@Override
	public Boolean getAggregatorKafkaForceParallelClear() {
		return getBoolean(AGGREGATOR_KAFKA_FORCE_PARALLEL_CLEAR);
	}
	
	public GeneralConfigImpl setAggregatorKafkaForceParallelClear(Boolean force) {
		String s = "";
		if ( force != null ) s = force ? "true" : "false";
		props.put(AGGREGATOR_KAFKA_FORCE_PARALLEL_CLEAR, s);
		return this;
	}
	
	@Override
	public long getAggregatorKafkaLingerMs() {
		return getLong(AGGREGATOR_KAFKA_LINGER_MS);
	}
	
	public GeneralConfigImpl setAggregatorKafkaLingerMs(long time) {
		props.put(AGGREGATOR_KAFKA_LINGER_MS, Long.toString(time));
		return this;
	}
	
	@Override
	public long getAggregatorKafkaStoreRetentionTime() {
		return getLong(AGGREGATOR_KAFKA_STORE_RETENTION_TIME);
	}
	
	public GeneralConfigImpl setAggregatorKafkaStoreRetentionTime(long time) {
		props.put(AGGREGATOR_KAFKA_STORE_RETENTION_TIME, Long.toString(time));
		return this;
	}
	
	@Override
	public int getAggregatorKafkaNumStreamThreads() {
		return getInt(AGGREGATOR_KAFKA_NUM_STREAM_THREADS);
	}
	
	public GeneralConfigImpl setAggregatorKafkaNumStreamThreads(int num) {
		props.put(AGGREGATOR_KAFKA_NUM_STREAM_THREADS, Integer.toString(num));
		return this;
	}
	
	@Override
	public String getItesymKafkaGroupId() {
		return getString(ITESYM_KAFKA_GROUP_ID);
	}
	
	public GeneralConfigImpl setItesymKafkaGroupId(String groupId) {
		props.put(ITESYM_KAFKA_GROUP_ID, groupId);
		return this;
	}

}
