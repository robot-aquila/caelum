package ru.prolib.caelum.aggregator.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class AggregatorRegistry {
	private final Periods periods;
	private final Map<Period, AggregatorEntry> entryByPeriod;

	AggregatorRegistry(Periods periods, Map<Period, AggregatorEntry> entry_by_period) {
		this.periods = periods;
		this.entryByPeriod = entry_by_period;
	}
	
	public AggregatorRegistry(Periods periods) {
		this(periods, new ConcurrentHashMap<>());
	}
	
	public AggregatorRegistry() {
		this(Periods.getInstance());
	}
	
	public Periods getPeriods() {
		return periods;
	}
	
	public Map<Period, AggregatorEntry> getEntryByPeriodMap() {
		return entryByPeriod;
	}
	
	public void register(AggregatorDescr descr, KafkaStreams streams) {
		switch ( descr.getType() ) {
		case ITEM:
		case TUPLE:
			break;
		default:
			throw new IllegalArgumentException("Aggregator of type is not allowed to register: " + descr.getType());
		}
		AggregatorEntry entry = new AggregatorEntry(descr, streams);
		// That actually doesn't matter who's exactly will provide the data
		entryByPeriod.put(descr.getPeriod(), entry);
	}
	
	public AggregatorEntry getByPeriod(Period period) {
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
	public AggregatorEntry findSuitableAggregatorToRebuildOnFly(Period period) {
		for ( Period sm_period : periods.getSmallerPeriodsThatCanFill(period) ) {
			AggregatorEntry entry = entryByPeriod.get(sm_period);
			if ( entry != null ) {
				return entry;
			}
		}
		throw new IllegalStateException("No suitable aggregator was found to rebuild: " + period);
	}
	
}
