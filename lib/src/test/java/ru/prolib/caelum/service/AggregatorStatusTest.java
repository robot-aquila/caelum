package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static ru.prolib.caelum.lib.Interval.*;
import static ru.prolib.caelum.service.AggregatorState.*;
import static ru.prolib.caelum.service.AggregatorType.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class AggregatorStatusTest {
	AggregatorStatus service;

	@Before
	public void setUp() throws Exception {
		service = new AggregatorStatus("AK", H3, TUPLE, RUNNING, "{ foobar }");
	}
	
	@Test
	public void testGetters() {
		assertEquals("AK", service.getImplCode());
		assertEquals(H3, service.getInterval());
		assertEquals(TUPLE, service.getType());
		assertEquals(RUNNING, service.getState());
		assertEquals("{ foobar }", service.getStatusInfo());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(815347, 117)
				.append("AK")
				.append(H3)
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
		assertTrue(service.equals(new AggregatorStatus("AK", H3, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new AggregatorStatus("KK", H3, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", H1, TUPLE, RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", H3, ITEM,  RUNNING, "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", H3, TUPLE, DEAD   , "{ foobar }")));
		assertFalse(service.equals(new AggregatorStatus("AK", H3, TUPLE, RUNNING, "{ barbar }")));
		assertFalse(service.equals(new AggregatorStatus("KK", H1, ITEM,  DEAD   , "{ barbar }")));
	}

	@Test
	public void testToString() {
		String expected = "AggregatorStatus[implCode=AK,interval=H3,type=TUPLE,state=RUNNING,statusInfo={ foobar }]";
		
		assertEquals(expected, service.toString());
	}

}
