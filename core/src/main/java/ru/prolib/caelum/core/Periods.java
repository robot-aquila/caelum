package ru.prolib.caelum.core;

import static ru.prolib.caelum.core.Period.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Periods {
	private static final Periods instance = new Periods();
	private static final Map<Period, Duration> durationMap;
	private static final Map<Period, List<Period>> hierarchyMap;
	
	static List<Period> periodsToFill(Period period) {
		List<Period> periods = new ArrayList<>(durationMap.keySet()), result = new ArrayList<>();
		Collections.reverse(periods);
		long period_millis = durationMap.get(period).toMillis();
		for ( Period s_period : periods ) {
			long s_period_millis = durationMap.get(s_period).toMillis();
			if ( s_period_millis < period_millis && period_millis % s_period_millis == 0 ) {
				result.add(s_period);
			}
		}
		return Collections.unmodifiableList(result);
	}
	
	static {
		Map<Period, Duration> dmap = new LinkedHashMap<>();
		dmap.put( M1, Duration.ofMinutes(1L));
		dmap.put( M2, Duration.ofMinutes(2L));
		dmap.put( M3, Duration.ofMinutes(3L));
		dmap.put( M5, Duration.ofMinutes(5L));
		dmap.put( M6, Duration.ofMinutes(6L));
		dmap.put(M10, Duration.ofMinutes(10L));
		dmap.put(M12, Duration.ofMinutes(12L));
		dmap.put(M15, Duration.ofMinutes(15L));
		dmap.put(M20, Duration.ofMinutes(20L));
		dmap.put(M30, Duration.ofMinutes(30L));
		dmap.put( H1, Duration.ofHours(1L));
		dmap.put( H2, Duration.ofHours(2L));
		dmap.put( H3, Duration.ofHours(3L));
		dmap.put( H4, Duration.ofHours(4L));
		dmap.put( H6, Duration.ofHours(6L));
		dmap.put( H8, Duration.ofHours(8L));
		dmap.put(H12, Duration.ofHours(12L));
		dmap.put( D1, Duration.ofDays(1L));
		durationMap = Collections.unmodifiableMap(dmap);

		Map<Period, List<Period>> fmap = new LinkedHashMap<>();
		for ( Period period : dmap.keySet() ) {
			fmap.put(period, periodsToFill(period));
		}
		hierarchyMap = Collections.unmodifiableMap(fmap);
	}
	
	public static Periods getInstance() {
		return instance;
	}

	public Duration getIntradayDuration(Period period) {
		Duration d = durationMap.get(period);
		if ( d == null ) {
			throw new NullPointerException("Period not found: " + period);
		}
		return d;
	}
	
	public Duration getIntradayDurationByCode(String code) {
		Period p = Period.valueOf(code);
		if ( p == null ) {
			throw new IllegalArgumentException("Unknown period: " + code);
		}
		return getIntradayDuration(p);
	}
	
	/**
	 * Get list of intraday period codes in order from smaller to bigger.
	 * <p>
	 * @return list of period codes
	 */
	public List<String> getIntradayPeriodCodes() {
		return durationMap.keySet().stream().map((v) -> v.toString()).collect(Collectors.toList());
	}
	
	/**
	 * Get list of intraday periods in order from from smaller to bigger.
	 * <p>
	 * @return list of periods
	 */
	public List<Period> getIntradayPeriods() {
		return durationMap.keySet().stream().collect(Collectors.toList());
	}
	
	/**
	 * Get list of smaller periods that can fill bigger period.
	 * <p>
	 * @param period - bigger period to fill
	 * @return list of smaller periods in order from bigger to smaller
	 */
	public List<Period> getSmallerPeriodsThatCanFill(Period period) {
		List<Period> result = hierarchyMap.get(period);
		if ( result == null ) {
			throw new IllegalStateException("Hierarchy map not defined: " + period);
		}
		return result;
	}

}
