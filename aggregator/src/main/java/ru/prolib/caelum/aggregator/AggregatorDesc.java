package ru.prolib.caelum.aggregator;

import ru.prolib.caelum.core.Period;

public class AggregatorDesc {
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
	public AggregatorDesc(AggregatorType type, Period period, String source, String target, String store_name) {
		this.type = type;
		this.period = period;
		this.source = source;
		this.target = target;
		this.storeName = store_name;
	}
	
	public AggregatorType getType() {
		return type;
	}
	
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
	
}
