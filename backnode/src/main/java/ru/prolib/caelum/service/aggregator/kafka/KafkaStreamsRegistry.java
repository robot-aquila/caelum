package ru.prolib.caelum.service.aggregator.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;

public class KafkaStreamsRegistry {
	private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsRegistry.class);
	
	private final HostInfo hostInfo;
	private final Intervals intervals;
	private final Map<Interval, KafkaAggregatorEntry> entryByInterval;

	KafkaStreamsRegistry(HostInfo hostInfo, Intervals intervals, Map<Interval, KafkaAggregatorEntry> entry_by_interval) {
		this.hostInfo = hostInfo;
		this.intervals = intervals;
		this.entryByInterval = entry_by_interval;
	}
	
	public KafkaStreamsRegistry(HostInfo hostInfo, Intervals intervals) {
		this(hostInfo, intervals, new ConcurrentHashMap<>());
	}
	
	public HostInfo getHostInfo() {
		return hostInfo;
	}
	
	public Intervals getIntervals() {
		return intervals;
	}
	
	public Map<Interval, KafkaAggregatorEntry> getEntryByIntervalMap() {
		return entryByInterval;
	}
	
	protected KafkaAggregatorEntry createEntry(KafkaAggregatorDescr descr, KafkaStreams streams) {
		return new KafkaAggregatorEntry(hostInfo, descr, streams, new KafkaStreamsAvailability());
	}
	
	/**
	 * Register streams of specified descriptor.
	 * <p>
	 * @param descr - streams descriptor
	 * @param streams - streams instance
	 */
	public void register(KafkaAggregatorDescr descr, KafkaStreams streams) {
		switch ( descr.getType() ) {
		case ITEM:
		case TUPLE:
			// That actually doesn't matter who's exactly will provide the data
			entryByInterval.put(descr.getInterval(), createEntry(descr, streams));
			break;
		default:
			throw new IllegalArgumentException("Aggregator of type is not allowed to register: " + descr.getType());
		}
	}
	
	/**
	 * Deregister streams of the specified descriptor.
	 * <p>
	 * @param descr - streams descriptor
	 */
	public void deregister(KafkaAggregatorDescr descr) {
		entryByInterval.remove(descr.getInterval());
	}
	
	public KafkaAggregatorEntry getByInterval(Interval interval) {
		return entryByInterval.get(interval);
	}

	/**
	 * Find aggregator of smaller interval to rebuild data of bigger interval.
	 * <p>
	 * If store not found and direct operation is not possible then find
	 * suitable store for smaller interval to make aggregation on-fly.
	 * <p>
	 * @param interval - interval that have to rebuilt
	 * @return an entry represented aggregator suitable to rebuild tuples of required interval
	 * @throws IllegalStateException - if suitable aggregator was not found
	 */
	public KafkaAggregatorEntry findSuitableAggregatorToRebuildOnFly(Interval interval) {
		for ( Interval sm_interval : intervals.getSmallerIntervalsThatCanFill(interval) ) {
			KafkaAggregatorEntry entry = entryByInterval.get(sm_interval);
			if ( entry != null ) {
				return entry;
			}
		}
		throw new IllegalStateException("No suitable aggregator was found to rebuild: " + interval);
	}
	
	public void setAvailability(KafkaAggregatorDescr descr, boolean is_available) {
		logger.debug("Streams availability change: {} -> {}", descr.interval, is_available);
		entryByInterval.get(descr.interval).setAvailable(is_available);
	}
	
}
