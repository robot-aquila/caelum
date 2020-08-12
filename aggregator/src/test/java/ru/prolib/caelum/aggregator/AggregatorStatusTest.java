package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;
import static ru.prolib.caelum.aggregator.AggregatorState.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.Period;

public class AggregatorStatusTest {
	AggregatorStatus service;

	@Before
	public void setUp() throws Exception {
		service = new AggregatorStatus("AK", Period.H3, TUPLE, RUNNING, "{ foobar }");
	}
	
	@Test
	public void testGetters() {
		assertEquals("AK", service.getImplCode());
		assertEquals(Period.H3, service.getPeriod());
		assertEquals(TUPLE, service.getType());
		assertEquals(RUNNING, service.getState());
		assertEquals("{ foobar }", service.getStatusInfo());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(815347, 117)
				.append("AK")
				.append(Period.H3)
				.append(TUPLE)
				.append(RUNNING)
				.append("{ foobar }")
				.build();

		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new AggregatorStatus("AK", Period.H3, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new AggregatorStatus("KK", Period.H3, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", Period.H1, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", Period.H3, ITEM,  RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", Period.H3, TUPLE, DEAD   , "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", Period.H3, TUPLE, RUNNING, "{ barbar }")));
		assertFalse(service.equals(new AggregatorStatus("KK", Period.H1, ITEM,  DEAD   , "{ barbar }")));
	}

	@Test
	public void testToString() {
		String expected = "AggregatorStatus[implCode=AK,period=H3,type=TUPLE,state=RUNNING,statusInfo={ foobar }]";
		
		assertEquals(expected, service.toString());
	}

}
