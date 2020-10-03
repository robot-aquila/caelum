package ru.prolib.caelum.service;

import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Intervals;

public abstract class GeneralConfig extends AbstractConfig {
	public abstract Intervals getIntervals();
	public abstract Mode getMode();
	public abstract String getHttpHost();
	public abstract int getHttpPort();
	public abstract HostInfo getHttpInfo();
	public abstract short getItemsTopicReplicationFactor();
	public abstract int getItemsTopicNumPartitions();
	public abstract long getItemsTopicRetentionTime();
	public abstract String getItemsTopicName();
	public abstract int getMaxErrors();
	public abstract long getDefaultTimeout();
	public abstract long getShutdownTimeout();
	public abstract String getKafkaBootstrapServers();
	public abstract String getKafkaStateDir();
	public abstract long getKafkaPollTimeout();
	public abstract String getFdbSubspace();
	public abstract String getFdbCluster();
	public abstract int getMaxItemsLimit();
	public abstract int getMaxSymbolsLimit();
	public abstract int getMaxEventsLimit();
	public abstract int getMaxTuplesLimit();
	public abstract String getItemServiceBuilder();
	public abstract String getItemServiceKafkaTransactionalId();
	public abstract String getSymbolServiceBuilder();
	public abstract String getSymbolServiceCategoryExtractor();
	public abstract String getAggregatorServiceBuilder();
	public abstract String getAggregatorInterval();
	public abstract String getAggregatorKafkaApplicationIdPrefix();
	public abstract String getAggregatorKafkaStorePrefix();
	public abstract String getAggregatorKafkaTargetTopicPrefix();
	public abstract String getAggregatorKafkaApplicationServer();
	public abstract Boolean getAggregatorKafkaForceParallelClear();
	public abstract long getAggregatorKafkaLingerMs();
	public abstract long getAggregatorKafkaStoreRetentionTime();
	public abstract int getAggregatorKafkaNumStreamThreads();
	public abstract String getItesymKafkaGroupId();
}
