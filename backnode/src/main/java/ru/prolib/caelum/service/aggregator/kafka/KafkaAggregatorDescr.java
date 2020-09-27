package ru.prolib.caelum.service.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.aggregator.AggregatorType;

public class KafkaAggregatorDescr {
	protected final AggregatorType type;
	protected final Interval interval;
	protected final String source, target, storeName;
	
	/**
	 * Constructor.
	 * <p>
	 * @param type - type of aggregation
	 * @param interval - interval of aggregation
	 * @param source - source topic. May be null in case of no source topic used.
	 * @param target - target topic. May be null in case if aggregated data do not stream to topic.
	 * @param store_name - store name to use for accessing data. 
	 */
	public KafkaAggregatorDescr(AggregatorType type, Interval interval, String source, String target, String store_name) {
		this.type = type;
		this.interval = interval;
		this.source = source;
		this.target = target;
		this.storeName = store_name;
	}
	
	public AggregatorType getType() {
		return type;
	}
	
	public Interval getInterval() {
		return interval;
	}
	
	public String getSource() {
		return source;
	}
	
	public String getTarget() {
		return target;
	}
	
	public String getStoreName() {
		return storeName;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("type", type)
				.append("interval", interval)
				.append("source", source)
				.append("target", target)
				.append("storeName", storeName)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(540441, 709)
				.append(type)
				.append(interval)
				.append(source)
				.append(target)
				.append(storeName)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorDescr.class ) {
			return false;
		}
		KafkaAggregatorDescr o = (KafkaAggregatorDescr) other;
		return new EqualsBuilder()
				.append(o.type, type)
				.append(o.interval, interval)
				.append(o.source, source)
				.append(o.target, target)
				.append(o.storeName, storeName)
				.build();
	}
	
}
