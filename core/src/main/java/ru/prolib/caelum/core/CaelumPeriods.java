package ru.prolib.caelum.core;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CaelumPeriods {
	private static final CaelumPeriods instance = new CaelumPeriods();
	private static final Map<String, Duration> PERIOD_MAP;
	
	static {
		Map<String, Duration> map = new LinkedHashMap<>();
		map.put("M1", Duration.ofMinutes(1L));
		map.put("M2", Duration.ofMinutes(2L));
		map.put("M3", Duration.ofMinutes(3L));
		map.put("M5", Duration.ofMinutes(5L));
		map.put("M6", Duration.ofMinutes(6L));
		map.put("M10", Duration.ofMinutes(10L));
		map.put("M12", Duration.ofMinutes(12L));
		map.put("M15", Duration.ofMinutes(15L));
		map.put("M20", Duration.ofMinutes(20L));
		map.put("M30", Duration.ofMinutes(30L));
		map.put("H1", Duration.ofHours(1L));
		map.put("H2", Duration.ofHours(2L));
		map.put("H3", Duration.ofHours(3L));
		map.put("H4", Duration.ofHours(4L));
		map.put("H6", Duration.ofHours(6L));
		map.put("H8", Duration.ofHours(8L));
		map.put("H12", Duration.ofHours(12L));
		map.put("D1", Duration.ofDays(1L));
		PERIOD_MAP = Collections.unmodifiableMap(map);
	}
	
	public static CaelumPeriods getInstance() {
		return instance;
	}
	
	public Duration getIntradayPeriodByCode(String code) {
		Duration p = PERIOD_MAP.get(code);
		if ( p == null ) {
			throw new IllegalArgumentException("Unknown period: " + code);
		}
		return p;
	}
	
	public Set<String> getIntradayPeriodCodes() {
		return PERIOD_MAP.keySet();
	}

}
