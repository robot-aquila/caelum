package ru.prolib.caelum.service.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.kafka.KafkaTuple;

public class KafkaAggregatorStoreInfo {
	private final boolean askAnotherHost;
	private final HostInfo hostInfo;
	private final ReadOnlyWindowStore<String, KafkaTuple> store;
	
	public KafkaAggregatorStoreInfo(HostInfo hostInfo, ReadOnlyWindowStore<String, KafkaTuple> store) {
		this.askAnotherHost = false;
		this.hostInfo = hostInfo;
		this.store = store;
	}
	
	public KafkaAggregatorStoreInfo(HostInfo hostInfo) {
		this.askAnotherHost = true;
		this.hostInfo = hostInfo;
		this.store = null;
	}
	
	public boolean askAnotherHost() {
		return askAnotherHost;
	}
	
	public HostInfo getHostInfo() {
		return hostInfo;
	}
	
	public ReadOnlyWindowStore<String, KafkaTuple> getStore() {
		return store;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7231121, 983)
				.append(askAnotherHost)
				.append(hostInfo)
				.append(store)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("askAnotherHost", askAnotherHost)
				.append("hostInfo", hostInfo)
				.append("store", store)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorStoreInfo.class ) {
			return false;
		}
		KafkaAggregatorStoreInfo o = (KafkaAggregatorStoreInfo) other;
		return new EqualsBuilder()
				.append(o.askAnotherHost, askAnotherHost)
				.append(o.hostInfo, hostInfo)
				.append(o.store, store)
				.build();
	}

}
