package ru.prolib.caelum.aggregator.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatedDataResponse;
import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.aggregator.IAggregator;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.aggregator.kafka.utils.WindowStoreIteratorLimited;
import ru.prolib.caelum.core.Interval;
import ru.prolib.caelum.core.Intervals;

public class KafkaAggregatorService implements IAggregatorService {
	public static final long MAX_TIME = Long.MAX_VALUE;
	
	static Instant T(long time) {
		return Instant.ofEpochMilli(time);
	}
	
	private final Intervals intervals;
	private final KafkaStreamsRegistry registry;
	private final List<IAggregator> aggregatorList;
	private final int maxLimit;
	private final boolean clearAggregatorsInParallel;
	private final long timeout;
	
	KafkaAggregatorService(Intervals intervals,
			KafkaStreamsRegistry registry,
			List<IAggregator> aggregatorList,
			int maxLimit,
			boolean clearAggregatorsInParallel,
			long timeout)
	{
		this.intervals = intervals;
		this.registry = registry;
		this.aggregatorList = aggregatorList;
		this.maxLimit = maxLimit;
		this.clearAggregatorsInParallel = clearAggregatorsInParallel;
		this.timeout = timeout;
	}
	
	public Intervals getIntervals() {
		return intervals;
	}
	
	public KafkaStreamsRegistry getRegistry() {
		return registry;
	}
	
	public List<IAggregator> getAggregatorList() {
		return aggregatorList;
	}
	
	public int getMaxLimit() {
		return maxLimit;
	}
	
	public boolean isClearAggregatorsInParallel() {
		return clearAggregatorsInParallel;
	}
	
	public long getTimeout() {
		return timeout;
	}
	
	private int getLimit(AggregatedDataRequest request) {
		Integer limit = request.getLimit();
		if ( limit == null ) {
			return maxLimit;
		} else {
			return Math.min(maxLimit, limit);
		}
	}
	
	private long getTime(Long requested_time, long default_time) {
		return requested_time == null ? default_time : requested_time;
	}
	
	private Duration getDuration(Interval interval) {
		return intervals.getIntervalDuration(interval);
	}
	
	@Override
	public AggregatedDataResponse fetch(AggregatedDataRequest request) {
		final String symbol = request.getSymbol();
		final Interval interval = request.getInterval();
		KafkaAggregatorEntry entry = registry.getByInterval(interval);
		long interval_millis = intervals.getIntervalDuration(interval).toMillis();
		long req_from = getTime(request.getFrom(), 0), req_to = getTime(request.getTo(), MAX_TIME);
		long from_align = req_from / interval_millis, to_align = req_to / interval_millis;
		if ( req_to % interval_millis > 0 ) {
			to_align ++;
		}
		long to_aligned = to_align * interval_millis - 1;
		long from_aligned = from_align * interval_millis;
		if ( to_aligned < 0 ) {
			to_aligned = MAX_TIME;
		}
		Instant from = T(from_aligned), to = T(to_aligned);
		WindowStoreIterator<KafkaTuple> it = null;
		KafkaAggregatorStoreInfo store_info = null;
		if ( entry == null ) {
			entry = registry.findSuitableAggregatorToRebuildOnFly(interval);
			store_info = entry.getStoreInfo(symbol, timeout);
			if ( store_info.askAnotherHost() == false ) {
				it = new KafkaTupleAggregateIterator(store_info.getStore().fetch(symbol, from, to), getDuration(interval));
			}
		} else {
			store_info = entry.getStoreInfo(symbol, timeout);
			if ( store_info.askAnotherHost() == false ) {
				it = store_info.getStore().fetch(symbol, from, to);
			}
		}
		return store_info.askAnotherHost() ?
			new AggregatedDataResponse(store_info.getHostInfo()) :
			new AggregatedDataResponse(
				store_info.getHostInfo(),
				new TupleIterator(symbol, new WindowStoreIteratorLimited<KafkaTuple>(it, getLimit(request)))
			);
	}
	
	protected CompletableFuture<Void> createClear(IAggregator aggregator, final boolean global) {
		return CompletableFuture.runAsync(() -> aggregator.clear(global));
	}

	@Override
	public void clear(boolean global) {
		// There is some kind problem with accessing files while streams cleanUp in Windows
		if ( clearAggregatorsInParallel  ) {
			int count = aggregatorList.size();
			CompletableFuture<?> f[] = new CompletableFuture<?>[count];
			for ( int i = 0; i < count; i ++ ) {
				f[i] = createClear(aggregatorList.get(i), global);
			}
			CompletableFuture.allOf(f).join();
		} else {
			for ( IAggregator aggregator : aggregatorList ) {
				aggregator.clear(global);
			}
		}
	}

	@Override
	public List<Interval> getAggregationIntervals() {
		return intervals.getIntervals();
	}

	@Override
	public List<AggregatorStatus> getAggregatorStatus() {
		final Map<Interval, AggregatorStatus> m = aggregatorList.stream()
			.map(x -> x.getStatus())
			.collect(Collectors.toMap(x -> x.getInterval(), x -> x));
		return intervals.getIntervals().stream()
			.filter(p -> m.containsKey(p))
			.map(p -> m.get(p))
			.collect(Collectors.toList());
	}

}
