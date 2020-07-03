package ru.prolib.caelum.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.aggregator.AggregatorType;
import ru.prolib.caelum.aggregator.IAggregatorDescr;
import ru.prolib.caelum.core.Period;

public class AggregatorDescr implements IAggregatorDescr {
	protected final AggregatorType type;
	protected final Period period;
	protected final String source, target, storeName;
	
	/**
	 * Constructor.
	 * <p>
	 * @param type - type of aggregation
	 * @param period - period of aggregation
	 * @param source - source topic. May be null in case of no source topic used.
	 * @param target - target topic. May be null in case if aggregated data do not stream to topic.
	 * @param store_name - store name to use for accessing data. 
	 */
	public AggregatorDescr(AggregatorType type, Period period, String source, String target, String store_name) {
		this.type = type;
		this.period = period;
		this.source = source;
		this.target = target;
		this.storeName = store_name;
	}
	
	@Override
	public AggregatorType getType() {
		return type;
	}
	
	@Override
	public Period getPeriod() {
		return period;
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
				.append("period", period)
				.append("source", source)
				.append("target", target)
				.append("storeName", storeName)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(540441, 709)
				.append(type)
				.append(period)
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
		if ( other == null || other.getClass() != AggregatorDescr.class ) {
			return false;
		}
		AggregatorDescr o = (AggregatorDescr) other;
		return new EqualsBuilder()
				.append(o.type, type)
				.append(o.period, period)
				.append(o.source, source)
				.append(o.target, target)
				.append(o.storeName, storeName)
				.build();
	}
	
}
