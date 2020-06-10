package ru.prolib.caelum.aggregator;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.core.Tuple;

public class AggregatorService {
	
	static class Entry {
		final AggregatorDesc desc;
		final KafkaStreams streams;
		
		Entry(AggregatorDesc desc, KafkaStreams streams) {
			this.desc = desc;
			this.streams = streams;
		}
	}
	
	private final Periods periods;
	private final Map<Period, Entry> entryByPeriod;
	private final Map<String, Entry> entryByStoreName;
	
	AggregatorService(Periods periods,
			ConcurrentHashMap<Period, Entry> entry_by_period,
			ConcurrentHashMap<String, Entry> entry_by_store)
	{
		this.periods = periods;
		this.entryByPeriod = entry_by_period;
		this.entryByStoreName = entry_by_store;
	}
	
	public AggregatorService() {
		this(Periods.getInstance(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
	}
	
	private ReadOnlyWindowStore<String, Tuple> getStoreByName(Entry entry) {
		String store_name = entry.desc.storeName;
		ReadOnlyWindowStore<String, Tuple> store = entry.streams.store(
				StoreQueryParameters.fromNameAndType(store_name,
				QueryableStoreTypes.windowStore())
			);
		if ( store == null ) {
			throw new IllegalStateException("Store not available: " + store_name);
		}
		return store;
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
	private Entry findSuitableAggregatorToRebuild(Period period) {
		for ( Period sm_period : periods.getSmallerPeriodsThatCanFill(period) ) {
			Entry entry = entryByPeriod.get(sm_period);
			if ( entry != null ) {
				return entry;
			}
		}
		throw new IllegalStateException("No suitable aggregator was found to rebuild: " + period);
	}
	
	public WindowStoreIterator<Tuple> fetch(AggregatedDataRequest request) {
		Period period = request.getPeriod();
		Entry entry = entryByPeriod.get(period);
		WindowStoreIterator<Tuple> it = null;
		if ( entry == null ) {
			entry = findSuitableAggregatorToRebuild(period);
			long period_millis = periods.getIntradayDuration(period).toMillis();
			long from_align = request.getFrom() / period_millis, to_align = request.getTo() / period_millis + 1;
			if ( request.getFrom() % period_millis > 0 ) {
				from_align ++;
			}
			Instant from = Instant.ofEpochMilli(from_align * period_millis),
					to = Instant.ofEpochMilli(to_align * period_millis).minusMillis(1);
			it = new TupleAggregateIterator(getStoreByName(entry).fetch(request.getSymbol(), from, to),
					periods.getIntradayDuration(period));
		} else {
			it = getStoreByName(entry).fetch(request.getSymbol(), request.getTimeFrom(), request.getTimeTo());
		}
		return new WindowStoreIteratorLimited<Tuple>(it, request.getLimit());
	}
	
	public void register(AggregatorDesc desc, KafkaStreams streams) {
		switch ( desc.type ) {
		case ITEM:
		case TUPLE:
			break;
		default:
			throw new IllegalArgumentException("Aggregator of type is not allowed to register: " + desc.type);
		}
		Entry entry = new Entry(desc, streams);
		// Mapping by store name is the first priority because it isn't conflict if two or more aggregators
		// with same period but different stores. But same store shouldn't be registered twice.
		if ( entryByStoreName.putIfAbsent(desc.storeName, entry) != null ) {
			throw new IllegalStateException("Aggregator associated with the store already exists: " + desc.storeName);
		}
		// Only a single aggregator is allowed for separate period of aggregation.
		if ( entryByPeriod.putIfAbsent(desc.period, entry) != null ) {
			entryByStoreName.remove(desc.storeName);
			throw new IllegalStateException("Aggregator associated with period already exists: " + desc.period);
		}
	}

}
