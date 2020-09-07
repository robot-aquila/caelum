package ru.prolib.caelum.core;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.time.Duration;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class IntervalTest {
	
	Interval service;

	@Before
	public void setUp() throws Exception {
		service = new Interval("D7", Duration.ofDays(7));
	}
	
	@Test
	public void testGetters() {
		assertEquals("D7", service.getCode());
		assertEquals(Duration.ofDays(7), service.getDuration());
		assertEquals(Duration.ofDays(7).toMillis(), service.getDurationMillis());
	}
	
	@Test
	public void testToString() {
		assertEquals("D7", service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(5134089, 201)
				.append("D7")
				.append(Duration.ofDays(7))
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new Interval("D7", Duration.ofDays(7))));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new Interval("D2", Duration.ofDays(7))));
		assertFalse(service.equals(new Interval("D1", Duration.ofDays(1))));
	}

	@Test
	public void testConstants() {
		assertEquals(new Interval("M1", Duration.ofMinutes(1)), Interval.M1);
		assertEquals(new Interval("M2", Duration.ofMinutes(2)), Interval.M2);
		assertEquals(new Interval("M3", Duration.ofMinutes(3)), Interval.M3);
		assertEquals(new Interval("M5", Duration.ofMinutes(5)), Interval.M5);
		assertEquals(new Interval("M6", Duration.ofMinutes(6)), Interval.M6);
		assertEquals(new Interval("M10", Duration.ofMinutes(10)), Interval.M10);
		assertEquals(new Interval("M12", Duration.ofMinutes(12)), Interval.M12);
		assertEquals(new Interval("M15", Duration.ofMinutes(15)), Interval.M15);
		assertEquals(new Interval("M20", Duration.ofMinutes(20)), Interval.M20);
		assertEquals(new Interval("M30", Duration.ofMinutes(30)), Interval.M30);
		assertEquals(new Interval("H1", Duration.ofHours(1)), Interval.H1);
		assertEquals(new Interval("H2", Duration.ofHours(2)), Interval.H2);
		assertEquals(new Interval("H3", Duration.ofHours(3)), Interval.H3);
		assertEquals(new Interval("H4", Duration.ofHours(4)), Interval.H4);
		assertEquals(new Interval("H6", Duration.ofHours(6)), Interval.H6);
		assertEquals(new Interval("H8", Duration.ofHours(8)), Interval.H8);
		assertEquals(new Interval("H12", Duration.ofHours(12)), Interval.H12);
		assertEquals(new Interval("D1", Duration.ofDays(1)), Interval.D1);
	}
	
	@Test
	public void testCompare() {
		assertThat(Interval.M5.compareTo(Interval.M1), is(greaterThan(0)));
		assertThat(Interval.M5.compareTo(Interval.M15), is(lessThan(0)));
		assertThat(Interval.M5.compareTo(Interval.M5), is(equalTo(0)));
		
		assertThat(Interval.M5.compareTo(new Interval("M6", Duration.ofMinutes(5))), is(lessThan(0)));
		assertThat(Interval.M5.compareTo(new Interval("M4", Duration.ofMinutes(5))), is(greaterThan(0)));
	}
	
}
