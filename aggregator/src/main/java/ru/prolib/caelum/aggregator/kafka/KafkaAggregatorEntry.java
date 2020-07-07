package ru.prolib.caelum.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

class KafkaAggregatorEntry {
	private final KafkaAggregatorDescr descr;
	private final KafkaStreams streams;
	
	KafkaAggregatorEntry(KafkaAggregatorDescr descr, KafkaStreams streams) {
		this.descr = descr;
		this.streams = streams;
	}
	
	public KafkaAggregatorDescr getDescriptor() {
		return descr;
	}
	
	public KafkaStreams getStreams() {
		return streams;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("descr", descr)
				.append("streams", streams)
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
				.append(o.descr, descr)
				.append(o.streams, streams)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1917, 11)
				.append(descr)
				.append(streams)
				.build();
	}
	
	public ReadOnlyWindowStore<String, KafkaTuple> getStore() {
		final String storeName = descr.getStoreName();
		ReadOnlyWindowStore<String, KafkaTuple> store = streams.store(
			StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore()));
		if ( store == null ) {
			throw new IllegalStateException("Store not available: " + storeName);
		}
		return store;
	}
	
}