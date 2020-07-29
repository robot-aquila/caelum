package ru.prolib.caelum.aggregator.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class KafkaStreamsRegistry {
	private final Periods periods;
	private final Map<Period, KafkaAggregatorEntry> entryByPeriod;

	KafkaStreamsRegistry(Periods periods, Map<Period, KafkaAggregatorEntry> entry_by_period) {
		this.periods = periods;
		this.entryByPeriod = entry_by_period;
	}
	
	public KafkaStreamsRegistry(Periods periods) {
		this(periods, new ConcurrentHashMap<>());
	}
	
	public Periods getPeriods() {
		return periods;
	}
	
	public Map<Period, KafkaAggregatorEntry> getEntryByPeriodMap() {
		return entryByPeriod;
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
			break;
		default:
			throw new IllegalArgumentException("Aggregator of type is not allowed to register: " + descr.getType());
		}
		KafkaAggregatorEntry entry = new KafkaAggregatorEntry(descr, streams);
		// That actually doesn't matter who's exactly will provide the data
		entryByPeriod.put(descr.getPeriod(), entry);
	}
	
	/**
	 * Deregister streams of the specified descriptor.
	 * <p>
	 * @param descr - streams descriptor
	 */
	public void deregister(KafkaAggregatorDescr descr) {
		entryByPeriod.remove(descr.getPeriod());
	}
	
	public KafkaAggregatorEntry getByPeriod(Period period) {
		return entryByPeriod.get(period);
	}

	/**
	 * Find aggregator of smaller period to rebuild data of bigger period.
	 * <p>
	 * If store not found and direct operation is not possible then find
	 * suitable store for smaller period to make aggregation on-fly.
	 * <p>
	 * @param period - period that have to rebuilt
	 * @return an entry represented aggregator suitable to rebuild tuples of required period
	 * @throws IllegalStateException - if suitable aggregator was not found
	 */
	public KafkaAggregatorEntry findSuitableAggregatorToRebuildOnFly(Period period) {
		for ( Period sm_period : periods.getSmallerPeriodsThatCanFill(period) ) {
			KafkaAggregatorEntry entry = entryByPeriod.get(sm_period);
			if ( entry != null ) {
				return entry;
			}
		}
		throw new IllegalStateException("No suitable aggregator was found to rebuild: " + period);
	}
	
}