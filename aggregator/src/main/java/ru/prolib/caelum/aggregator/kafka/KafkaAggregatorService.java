package ru.prolib.caelum.aggregator.kafka;

import java.time.Instant;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.WindowStoreIterator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.aggregator.kafka.utils.WindowStoreIteratorLimited;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class KafkaAggregatorService implements IAggregatorService {
	
	static Instant T(long time) {
		return Instant.ofEpochMilli(time);
	}
	
	private final Periods periods;
	private final KafkaAggregatorRegistry registry;
	private final int maxLimit;
	
	KafkaAggregatorService(Periods periods, KafkaAggregatorRegistry registry, int maxLimit) {
		this.periods = periods;
		this.registry = registry;
		this.maxLimit = maxLimit;
	}
	
	public KafkaAggregatorService(Periods periods, int maxLimit) {
		this(periods, new KafkaAggregatorRegistry(periods), maxLimit);
	}
	
	public KafkaAggregatorService(int maxLimit) {
		this(Periods.getInstance(), maxLimit);
	}
	
	public Periods getPeriods() {
		return periods;
	}
	
	public KafkaAggregatorRegistry getRegistry() {
		return registry;
	}
	
	public int getMaxLimit() {
		return maxLimit;
	}
	
	@Override
	public ICloseableIterator<ITuple> fetch(AggregatedDataRequest request) {
		final String symbol = request.getSymbol();
		final Period period = request.getPeriod();
		KafkaAggregatorEntry entry = registry.getByPeriod(period);
		long period_millis = periods.getIntradayDuration(period).toMillis();
		long from_align = request.getFrom() / period_millis, to_align = request.getTo() / period_millis;
		if ( request.getTo() % period_millis > 0 ) {
			to_align ++;
		}
		Instant from = T(from_align * period_millis), to = T(to_align * period_millis).minusMillis(1);
		WindowStoreIterator<KafkaTuple> it = null;
		if ( entry == null ) {
			entry = registry.findSuitableAggregatorToRebuildOnFly(period);
			it = new KafkaTupleAggregateIterator(entry.getStore().fetch(symbol, from, to),
					periods.getIntradayDuration(period));
		} else {
			it = entry.getStore().fetch(symbol, from, to);
		}
		return new TupleIterator(symbol,
				new WindowStoreIteratorLimited<KafkaTuple>(it, Math.min(maxLimit, request.getLimit())));
	}
	
	public void register(KafkaAggregatorDescr descr, KafkaStreams streams) {
		registry.register(descr, streams);
	}

}
