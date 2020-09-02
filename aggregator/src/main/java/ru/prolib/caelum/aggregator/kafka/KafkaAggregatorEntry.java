package ru.prolib.caelum.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.HostInfo;

class KafkaAggregatorEntry {
	static final Logger logger = LoggerFactory.getLogger(KafkaAggregatorEntry.class);
	
	static org.apache.kafka.streams.state.HostInfo convert(HostInfo hostInfo) {
		return new org.apache.kafka.streams.state.HostInfo(hostInfo.getHost(), hostInfo.getPort());
	}
	
	static HostInfo convert(org.apache.kafka.streams.state.HostInfo hostInfo) {
		return new HostInfo(hostInfo.host(), hostInfo.port());
	}
	
	private final HostInfo hostInfo;
	private final org.apache.kafka.streams.state.HostInfo akHostInfo;
	private final KafkaAggregatorDescr descr;
	private final KafkaStreams streams;
	private final KafkaStreamsAvailability state;
	
	KafkaAggregatorEntry(HostInfo hostInfo,
			KafkaAggregatorDescr descr,
			KafkaStreams streams,
			KafkaStreamsAvailability state)
	{
		this.hostInfo = hostInfo;
		this.akHostInfo = convert(hostInfo);
		this.descr = descr;
		this.streams = streams;
		this.state = state;
	}
	
	/**
	 * Get host info representing this host itself.
	 * <p>
	 * @return host info
	 */
	public HostInfo getHostInfo() {
		return hostInfo;
	}
	
	public KafkaAggregatorDescr getDescriptor() {
		return descr;
	}
	
	public KafkaStreams getStreams() {
		return streams;
	}
	
	public KafkaStreamsAvailability getState() {
		return state;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("hostInfo", hostInfo)
				.append("descr", descr)
				.append("streams", streams)
				.append("state", state)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorEntry.class ) {
			return false;
		}
		KafkaAggregatorEntry o = (KafkaAggregatorEntry) other;
		return new EqualsBuilder()
				.append(o.hostInfo, hostInfo)
				.append(o.descr, descr)
				.append(o.streams, streams)
				.append(o.state, state)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1917, 11)
				.append(hostInfo)
				.append(descr)
				.append(streams)
				.append(state)
				.build();
	}
	
	private ReadOnlyWindowStore<String, KafkaTuple> getStoreOrNull() {
		QueryableStoreType<ReadOnlyWindowStore<String, KafkaTuple>> type = QueryableStoreTypes.windowStore();
		return streams.store(StoreQueryParameters.fromNameAndType(descr.getStoreName(), type).enableStaleStores());
	}
	
	public ReadOnlyWindowStore<String, KafkaTuple> getStore() {
		ReadOnlyWindowStore<String, KafkaTuple> store = getStoreOrNull();
		if ( store == null ) {
			throw new IllegalStateException("Store not available: " + descr.getStoreName());
		}
		return store;
	}
	
	public ReadOnlyWindowStore<String, KafkaTuple> getStore(long timeout) {
		if ( state.waitForChange(true, timeout) ) {
			return getStore();
		} else {
			throw new IllegalStateException("Timeout while awaiting store availability: " + descr.getStoreName());
		}
	}
	
	public KafkaAggregatorStoreInfo getStoreInfo(String key, long timeout) {
		if ( ! state.waitForChange(true, timeout) ) {
			throw new IllegalStateException("Timeout while awaiting store availability: " + descr.getStoreName());
		}
		KeyQueryMetadata md =
				streams.queryMetadataForKey(descr.getStoreName(), key, KafkaTupleSerdes.keySerde().serializer());
		if ( md.getActiveHost().equals(KeyQueryMetadata.NOT_AVAILABLE.getActiveHost()) == true ) {
			throw new IllegalStateException("Metadata not available: store=" + descr.getStoreName() + " key=" + key);
		}
		return akHostInfo.equals(md.getActiveHost()) || md.getStandbyHosts().contains(akHostInfo) ?
			new KafkaAggregatorStoreInfo(hostInfo, getStore()) :
			new KafkaAggregatorStoreInfo(convert(md.getActiveHost()));
	}
	
	public void setAvailable(boolean is_available) {
		state.setAvailable(is_available);
	}
	
	public boolean isAvailable() {
		return state.isAvailable();
	}
	
	public KafkaStreams.State getStreamsState() {
		return streams.state();
	}
}