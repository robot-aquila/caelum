package ru.prolib.caelum.core;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.Period.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class PeriodsTest {
	Periods service;

	@Before
	public void setUp() throws Exception {
		service = Periods.getInstance();
	}
	
	@Test
	public void testGetIntradayPeriodCodes() {
		assertEquals(Arrays.asList("M1", "M2", "M3", "M5", "M6", "M10", "M12", "M15",
				"M20", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D1"),
				service.getIntradayPeriodCodes());
	}
	
	@Test
	public void testGetIntradayPeriods() {
		assertEquals(Arrays.asList(M1, M2, M3, M5, M6, M10, M12, M15,
				M20, M30, H1, H2, H3, H4, H6, H8, H12, D1),
				service.getIntradayPeriods());
	}
	
	@Test
	public void testGetIntradayDuration() {
		assertEquals(Duration.ofMinutes( 1), service.getIntradayDuration(M1));
		assertEquals(Duration.ofMinutes( 2), service.getIntradayDuration(M2));
		assertEquals(Duration.ofMinutes( 3), service.getIntradayDuration(M3));
		assertEquals(Duration.ofMinutes( 5), service.getIntradayDuration(M5));
		assertEquals(Duration.ofMinutes( 6), service.getIntradayDuration(M6));
		assertEquals(Duration.ofMinutes(10), service.getIntradayDuration(M10));
		assertEquals(Duration.ofMinutes(12), service.getIntradayDuration(M12));
		assertEquals(Duration.ofMinutes(15), service.getIntradayDuration(M15));
		assertEquals(Duration.ofMinutes(20), service.getIntradayDuration(M20));
		assertEquals(Duration.ofMinutes(30), service.getIntradayDuration(M30));
		assertEquals(Duration.ofHours( 1), service.getIntradayDuration(H1));
		assertEquals(Duration.ofHours( 2), service.getIntradayDuration(H2));
		assertEquals(Duration.ofHours( 3), service.getIntradayDuration(H3));
		assertEquals(Duration.ofHours( 4), service.getIntradayDuration(H4));
		assertEquals(Duration.ofHours( 6), service.getIntradayDuration(H6));
		assertEquals(Duration.ofHours( 8), service.getIntradayDuration(H8));
		assertEquals(Duration.ofHours(12), service.getIntradayDuration(H12));
		assertEquals(Duration.ofDays(1), service.getIntradayDuration(D1));
		assertEquals(18, service.getIntradayPeriodCodes().size());
	}

	@Test
	public void testGetIntradayDurationByCode() {
		assertEquals(Duration.ofMinutes( 1), service.getIntradayDurationByCode("M1"));
		assertEquals(Duration.ofMinutes( 2), service.getIntradayDurationByCode("M2"));
		assertEquals(Duration.ofMinutes( 3), service.getIntradayDurationByCode("M3"));
		assertEquals(Duration.ofMinutes( 5), service.getIntradayDurationByCode("M5"));
		assertEquals(Duration.ofMinutes( 6), service.getIntradayDurationByCode("M6"));
		assertEquals(Duration.ofMinutes(10), service.getIntradayDurationByCode("M10"));
		assertEquals(Duration.ofMinutes(12), service.getIntradayDurationByCode("M12"));
		assertEquals(Duration.ofMinutes(15), service.getIntradayDurationByCode("M15"));
		assertEquals(Duration.ofMinutes(20), service.getIntradayDurationByCode("M20"));
		assertEquals(Duration.ofMinutes(30), service.getIntradayDurationByCode("M30"));
		assertEquals(Duration.ofHours( 1), service.getIntradayDurationByCode("H1"));
		assertEquals(Duration.ofHours( 2), service.getIntradayDurationByCode("H2"));
		assertEquals(Duration.ofHours( 3), service.getIntradayDurationByCode("H3"));
		assertEquals(Duration.ofHours( 4), service.getIntradayDurationByCode("H4"));
		assertEquals(Duration.ofHours( 6), service.getIntradayDurationByCode("H6"));
		assertEquals(Duration.ofHours( 8), service.getIntradayDurationByCode("H8"));
		assertEquals(Duration.ofHours(12), service.getIntradayDurationByCode("H12"));
		assertEquals(Duration.ofDays(1), service.getIntradayDurationByCode("D1"));
		assertEquals(18, service.getIntradayPeriodCodes().size());
	}
	
	@Test
	public void testGetSmallerPeriodsThatCanFill() {
		Map<Period, List<Period>> expected = new HashMap<>();
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
		for ( Period period : service.getIntradayPeriods() ) {
			assertTrue("Period not defined: " + period, expected.containsKey(period));
			assertEquals("Unexpected list for period: " + period,
					expected.get(period), service.getSmallerPeriodsThatCanFill(period));
		}
	}

}
