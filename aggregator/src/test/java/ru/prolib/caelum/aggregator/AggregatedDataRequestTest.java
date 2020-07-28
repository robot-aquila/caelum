package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;

import java.time.Instant;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.Period;

public class AggregatedDataRequestTest {
	AggregatedDataRequest service;

	@Before
	public void setUp() throws Exception {
		service = new AggregatedDataRequest("foobar", Period.H1, 2000L, 5000L, 500);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foobar", service.getSymbol());
		assertEquals(Period.H1, service.getPeriod());
		assertEquals(Long.valueOf(2000L), service.getFrom());
		assertEquals(Long.valueOf(5000L), service.getTo());
		assertEquals(Integer.valueOf(500), service.getLimit());
		assertEquals(Instant.ofEpochMilli(2000L), service.getTimeFrom());
		assertEquals(Instant.ofEpochMilli(5000L), service.getTimeTo());
	}
	
	@Test
	public void testCtor_NullParamsAllowed() {
		service = new AggregatedDataRequest("barbar", Period.M15, null, null, null);
		assertEquals("barbar", service.getSymbol());
		assertEquals(Period.M15, service.getPeriod());
		assertNull(service.getFrom());
		assertNull(service.getTimeFrom());
		assertNull(service.getTimeTo());
		assertNull(service.getTimeTo());
	}
	
	@Test
	public void testToString() {
		String expected = "AggregatedDataRequest[symbol=foobar,period=H1,from=2000,to=5000,limit=500]";
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(780011759, 75)
				.append("foobar")
				.append(Period.H1)
				.append(2000L)
				.append(5000L)
				.append(500)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals_SpecialCases() {
		assertTrue(service.equals(service));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(new AggregatedDataRequest("foobar", Period.H1, 2000L, 5000L, 500)));
		assertFalse(service.equals(new AggregatedDataRequest("gammar", Period.H1, 2000L, 5000L, 500)));
		assertFalse(service.equals(new AggregatedDataRequest("foobar", Period.H2, 2000L, 5000L, 500)));
		assertFalse(service.equals(new AggregatedDataRequest("foobar", Period.H1, 1000L, 5000L, 500)));
		assertFalse(service.equals(new AggregatedDataRequest("foobar", Period.H1, 2000L, 7000L, 500)));
		assertFalse(service.equals(new AggregatedDataRequest("foobar", Period.H1, 2000L, 5000L, 800)));
		assertFalse(service.equals(new AggregatedDataRequest("gammar", Period.H2, 1000L, 7000L, 800)));
	}

}
