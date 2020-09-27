package ru.prolib.caelum.lib;

import java.time.Duration;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Interval implements Comparable<Interval> {
	
	public static final Interval
		M1  = new Interval("M1", Duration.ofMinutes(1)),
		M2  = new Interval("M2", Duration.ofMinutes(2)),
		M3  = new Interval("M3", Duration.ofMinutes(3)),
		M5  = new Interval("M5", Duration.ofMinutes(5)),
		M6  = new Interval("M6", Duration.ofMinutes(6)),
		M10 = new Interval("M10", Duration.ofMinutes(10)),
		M12 = new Interval("M12", Duration.ofMinutes(12)),
		M15 = new Interval("M15", Duration.ofMinutes(15)),
		M20 = new Interval("M20", Duration.ofMinutes(20)),
		M30 = new Interval("M30", Duration.ofMinutes(30)),
		H1  = new Interval("H1", Duration.ofHours(1)),
		H2  = new Interval("H2", Duration.ofHours(2)),
		H3  = new Interval("H3", Duration.ofHours(3)),
		H4  = new Interval("H4", Duration.ofHours(4)),
		H6  = new Interval("H6", Duration.ofHours(6)),
		H8  = new Interval("H8", Duration.ofHours(8)),
		H12 = new Interval("H12", Duration.ofHours(12)),
		D1  = new Interval("D1", Duration.ofDays(1));
	
	private final String code;
	private final Duration duration;
	
	public Interval(String code, Duration duration) {
		this.code = code;
		this.duration = duration;
	}
	
	public String getCode() {
		return code;
	}
	
	public Duration getDuration() {
		return duration;
	}
	
	public long getDurationMillis() {
		return duration.toMillis();
	}
	
	@Override
	public String toString() {
		return code;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(5134089, 201)
				.append(code)
				.append(duration)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Interval.class ) {
			return false;
		}
		Interval o = (Interval) other;
		return new EqualsBuilder()
				.append(o.code, code)
				.append(o.duration, duration)
				.build();
	}

	@Override
	public int compareTo(Interval other) {
		int x = duration.compareTo(other.duration);
		if ( x != 0 ) return x;
		return code.compareTo(other.code);
	}
	
}
