package ru.prolib.caelum.core;

import static ru.prolib.caelum.core.Period.*;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Periods {
	private static final Periods instance = new Periods();
	private static final Map<Period, Duration> PERIOD_MAP;
	
	static {
		Map<Period, Duration> map = new LinkedHashMap<>();
		map.put( M1, Duration.ofMinutes(1L));
		map.put( M2, Duration.ofMinutes(2L));
		map.put( M3, Duration.ofMinutes(3L));
		map.put( M5, Duration.ofMinutes(5L));
		map.put( M6, Duration.ofMinutes(6L));
		map.put(M10, Duration.ofMinutes(10L));
		map.put(M12, Duration.ofMinutes(12L));
		map.put(M15, Duration.ofMinutes(15L));
		map.put(M20, Duration.ofMinutes(20L));
		map.put(M30, Duration.ofMinutes(30L));
		map.put( H1, Duration.ofHours(1L));
		map.put( H2, Duration.ofHours(2L));
		map.put( H3, Duration.ofHours(3L));
		map.put( H4, Duration.ofHours(4L));
		map.put( H6, Duration.ofHours(6L));
		map.put( H8, Duration.ofHours(8L));
		map.put(H12, Duration.ofHours(12L));
		map.put( D1, Duration.ofDays(1L));
		PERIOD_MAP = Collections.unmodifiableMap(map);
	}
	
	public static Periods getInstance() {
		return instance;
	}

	public Duration getIntradayDuration(Period period) {
		Duration d = PERIOD_MAP.get(period);
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
	
	public List<String> getIntradayPeriodCodes() {
		return PERIOD_MAP.keySet().stream().map((v) -> v.toString()).collect(Collectors.toList());
	}

}
