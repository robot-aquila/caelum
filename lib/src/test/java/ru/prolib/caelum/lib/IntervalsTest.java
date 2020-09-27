package ru.prolib.caelum.lib;

import static org.junit.Assert.*;
import static ru.prolib.caelum.lib.Interval.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class IntervalsTest {
	Intervals service;

	@Before
	public void setUp() throws Exception {
		service = new Intervals();
	}
	
	@Test
	public void testGetIntervalByCode() {
		assertSame(M1, service.getIntervalByCode("M1"));
		assertSame(H3, service.getIntervalByCode("H3"));
		assertSame(D1, service.getIntervalByCode("D1"));
	}
	
	@Test
	public void tyestGetIntervalByCode_ShouldThrowIfNotFound() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getIntervalByCode("X"));
		
		assertEquals("Interval not found: X", e.getMessage());
	}
	
	@Test
	public void testGetIntervalCodes() {
		assertEquals(Arrays.asList("M1", "M2", "M3", "M5", "M6", "M10", "M12", "M15",
				"M20", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D1"),
				service.getIntervalCodes());
	}
	
	@Test
	public void testGetIntervals() {
		assertEquals(Arrays.asList(M1, M2, M3, M5, M6, M10, M12, M15,
				M20, M30, H1, H2, H3, H4, H6, H8, H12, D1),
				service.getIntervals());
	}
	
	@Test
	public void testGetIntervalDuration() {
		assertEquals(Duration.ofMinutes( 1), service.getIntervalDuration(M1));
		assertEquals(Duration.ofMinutes( 2), service.getIntervalDuration(M2));
		assertEquals(Duration.ofMinutes( 3), service.getIntervalDuration(M3));
		assertEquals(Duration.ofMinutes( 5), service.getIntervalDuration(M5));
		assertEquals(Duration.ofMinutes( 6), service.getIntervalDuration(M6));
		assertEquals(Duration.ofMinutes(10), service.getIntervalDuration(M10));
		assertEquals(Duration.ofMinutes(12), service.getIntervalDuration(M12));
		assertEquals(Duration.ofMinutes(15), service.getIntervalDuration(M15));
		assertEquals(Duration.ofMinutes(20), service.getIntervalDuration(M20));
		assertEquals(Duration.ofMinutes(30), service.getIntervalDuration(M30));
		assertEquals(Duration.ofHours( 1), service.getIntervalDuration(H1));
		assertEquals(Duration.ofHours( 2), service.getIntervalDuration(H2));
		assertEquals(Duration.ofHours( 3), service.getIntervalDuration(H3));
		assertEquals(Duration.ofHours( 4), service.getIntervalDuration(H4));
		assertEquals(Duration.ofHours( 6), service.getIntervalDuration(H6));
		assertEquals(Duration.ofHours( 8), service.getIntervalDuration(H8));
		assertEquals(Duration.ofHours(12), service.getIntervalDuration(H12));
		assertEquals(Duration.ofDays(1), service.getIntervalDuration(D1));
		assertEquals(18, service.getIntervalCodes().size());
	}

	@Test
	public void testGetIntervalDurationByCode() {
		assertEquals(Duration.ofMinutes( 1), service.getIntervalDurationByCode("M1"));
		assertEquals(Duration.ofMinutes( 2), service.getIntervalDurationByCode("M2"));
		assertEquals(Duration.ofMinutes( 3), service.getIntervalDurationByCode("M3"));
		assertEquals(Duration.ofMinutes( 5), service.getIntervalDurationByCode("M5"));
		assertEquals(Duration.ofMinutes( 6), service.getIntervalDurationByCode("M6"));
		assertEquals(Duration.ofMinutes(10), service.getIntervalDurationByCode("M10"));
		assertEquals(Duration.ofMinutes(12), service.getIntervalDurationByCode("M12"));
		assertEquals(Duration.ofMinutes(15), service.getIntervalDurationByCode("M15"));
		assertEquals(Duration.ofMinutes(20), service.getIntervalDurationByCode("M20"));
		assertEquals(Duration.ofMinutes(30), service.getIntervalDurationByCode("M30"));
		assertEquals(Duration.ofHours( 1), service.getIntervalDurationByCode("H1"));
		assertEquals(Duration.ofHours( 2), service.getIntervalDurationByCode("H2"));
		assertEquals(Duration.ofHours( 3), service.getIntervalDurationByCode("H3"));
		assertEquals(Duration.ofHours( 4), service.getIntervalDurationByCode("H4"));
		assertEquals(Duration.ofHours( 6), service.getIntervalDurationByCode("H6"));
		assertEquals(Duration.ofHours( 8), service.getIntervalDurationByCode("H8"));
		assertEquals(Duration.ofHours(12), service.getIntervalDurationByCode("H12"));
		assertEquals(Duration.ofDays(1), service.getIntervalDurationByCode("D1"));
		assertEquals(18, service.getIntervalCodes().size());
	}
	
	@Test
	public void testGetSmallerIntervalsThatCanFill() {
		Map<Interval, List<Interval>> expected = new HashMap<>();
		expected.put( M1, Arrays.asList());
		expected.put( M2, Arrays.asList( M1));
		expected.put( M3, Arrays.asList( M1));
		expected.put( M5, Arrays.asList( M1));
		expected.put( M6, Arrays.asList( M3,  M2,  M1));
		expected.put(M10, Arrays.asList( M5,  M2,  M1));
		expected.put(M12, Arrays.asList( M6,  M3,  M2,  M1));
		expected.put(M15, Arrays.asList( M5,  M3,  M1));
		expected.put(M20, Arrays.asList(M10,  M5,  M2,  M1));
		expected.put(M30, Arrays.asList(M15, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H1, Arrays.asList(M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H2, Arrays.asList( H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H3, Arrays.asList( H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H4, Arrays.asList( H2,  H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H6, Arrays.asList( H3,  H2,  H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( H8, Arrays.asList( H4,  H2,  H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put(H12, Arrays.asList( H6,  H4,  H3,  H2,  H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		expected.put( D1, Arrays.asList(H12,  H8,  H6,  H4,  H3,  H2,  H1, M30, M20, M15, M12, M10,  M6,  M5,  M3,  M2,  M1));
		for ( Interval interval : service.getIntervals() ) {
			assertTrue("Interval not defined: " + interval, expected.containsKey(interval));
			assertEquals("Unexpected list for interval: " + interval,
					expected.get(interval), service.getSmallerIntervalsThatCanFill(interval));
		}
	}

}
