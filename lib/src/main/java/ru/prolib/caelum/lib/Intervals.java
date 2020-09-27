package ru.prolib.caelum.lib;

import static ru.prolib.caelum.lib.Interval.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Intervals {
	
	static List<Interval> getIntervalsInside(Interval of_interval, List<Interval> intervals_in_rev_order) {
		List<Interval> result = new ArrayList<>();
		long of_interval_millis = of_interval.getDurationMillis();
		for ( Interval interval : intervals_in_rev_order ) {
			long interval_millis = interval.getDurationMillis();
			if ( interval_millis < of_interval_millis && of_interval_millis % interval_millis == 0 ) {
				result.add(interval);
			}
		}
		return result;
	}
	
	private final Set<Interval> checkMap;
	private final Map<String, Interval> intervalByCode;
	private final List<Interval> sortedIntervals;
	private final Map<String, List<Interval>> hierarchyMapByCode;

	public Intervals(Collection<Interval> intervals) {
		this.checkMap = new HashSet<>();
		this.intervalByCode = new HashMap<>();
		this.hierarchyMapByCode = new HashMap<>();
		this.sortedIntervals = new ArrayList<>(intervals);
		Collections.sort(this.sortedIntervals);
		List<Interval> reversed_list = new ArrayList<>(this.sortedIntervals);
		Collections.reverse(reversed_list);
		for ( Interval interval : this.sortedIntervals ) {
			this.checkMap.add(interval);
			this.intervalByCode.put(interval.getCode(), interval);
			this.hierarchyMapByCode.put(interval.getCode(), getIntervalsInside(interval, reversed_list));
		}
	}
	
	public Intervals() {
		this(Arrays.asList(M1, M2, M3, M5, M6, M10, M12, M15, M20, M30, H1, H2, H3, H4, H6, H8, H12, D1));
	}
	
	public Interval getIntervalByCode(String interval_code) {
		Interval interval = intervalByCode.get(interval_code);
		if ( interval == null ) {
			throw new IllegalArgumentException("Interval not found: " + interval_code);
		}
		return interval;
	}
	
	public Duration getIntervalDuration(Interval interval) {
		if ( checkMap.contains(interval) == false ) {
			throw new NullPointerException("Interval not found: " + interval);
		}
		return interval.getDuration();
	}
	
	public Duration getIntervalDurationByCode(String interval_code) {
		return getIntervalByCode(interval_code).getDuration();
	}
	
	/**
	 * Get interval codes in order from smaller to bigger.
	 * <p>
	 * @return list of interval codes
	 */
	public List<String> getIntervalCodes() {
		return sortedIntervals.stream().map(x -> x.getCode()).collect(Collectors.toList());
	}
	
	/**
	 * Get intervals in order from from smaller to bigger.
	 * <p>
	 * @return list of intervals
	 */
	public List<Interval> getIntervals() {
		return Collections.unmodifiableList(sortedIntervals);
	}
	
	/**
	 * Get list of smaller intervals that can fill bigger interval.
	 * <p>
	 * @param interval - bigger interval to fill
	 * @return list of smaller intervals in order from bigger to smaller
	 */
	public List<Interval> getSmallerIntervalsThatCanFill(Interval interval) {
		getIntervalDuration(interval);
		List<Interval> result = hierarchyMapByCode.get(interval.getCode());
		if ( result == null ) {
			throw new IllegalStateException("No hierarchy for interval: " + interval);
		}
		return result;
	}

}
