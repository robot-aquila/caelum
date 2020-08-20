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
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class KafkaAggregatorService implements IAggregatorService {
	public static final long MAX_TIME = Long.MAX_VALUE;
	
	static Instant T(long time) {
		return Instant.ofEpochMilli(time);
	}
	
	private final Periods periods;
	private final KafkaStreamsRegistry registry;
	private final List<IAggregator> aggregatorList;
	private final int maxLimit;
	private final boolean clearAggregatorsInParallel;
	private final long timeout;
	
	KafkaAggregatorService(Periods periods,
			KafkaStreamsRegistry registry,
			List<IAggregator> aggregatorList,
			int maxLimit,
			boolean clearAggregatorsInParallel,
			long timeout)
	{
		this.periods = periods;
		this.registry = registry;
		this.aggregatorList = aggregatorList;
		this.maxLimit = maxLimit;
		this.clearAggregatorsInParallel = clearAggregatorsInParallel;
		this.timeout = timeout;
	}
	
	public Periods getPeriods() {
		return periods;
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
	
	private Duration getDuration(Period period) {
		return periods.getIntradayDuration(period);
	}
	
	@Override
	public AggregatedDataResponse fetch(AggregatedDataRequest request) {
		final String symbol = request.getSymbol();
		final Period period = request.getPeriod();
		KafkaAggregatorEntry entry = registry.getByPeriod(period);
		long period_millis = periods.getIntradayDuration(period).toMillis();
		long req_from = getTime(request.getFrom(), 0), req_to = getTime(request.getTo(), MAX_TIME);
		long from_align = req_from / period_millis, to_align = req_to / period_millis;
		if ( req_to % period_millis > 0 ) {
			to_align ++;
		}
		long to_aligned = to_align * period_millis - 1;
		long from_aligned = from_align * period_millis;
		if ( to_aligned < 0 ) {
			to_aligned = MAX_TIME;
		}
		Instant from = T(from_aligned), to = T(to_aligned);
		WindowStoreIterator<KafkaTuple> it = null;
		KafkaAggregatorStoreInfo store_info = null;
		if ( entry == null ) {
			entry = registry.findSuitableAggregatorToRebuildOnFly(period);
			store_info = entry.getStoreInfo(symbol, timeout);
			if ( store_info.askAnotherHost() == false ) {
				it = new KafkaTupleAggregateIterator(store_info.getStore().fetch(symbol, from, to), getDuration(period));
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
	public List<Period> getAggregationPeriods() {
		return periods.getIntradayPeriods();
	}

	@Override
	public List<AggregatorStatus> getAggregatorStatus() {
		final Map<Period, AggregatorStatus> m = aggregatorList.stream()
			.map(x -> x.getStatus())
			.collect(Collectors.toMap(x -> x.getPeriod(), x -> x));
		return periods.getIntradayPeriods().stream()
			.filter(p -> m.containsKey(p))
			.map(p -> m.get(p))
			.collect(Collectors.toList());
	}

}
