package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.AggregatorState;
import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.AggregatorType;

public class AggregatorStatusMvcAdapterTest {
	Object statusInfo;
	AggregatorStatus status;
	AggregatorStatusMvcAdapter service;

	@Before
	public void setUp() throws Exception {
		statusInfo = new Object();
		status = new AggregatorStatus("boom", Interval.M15, AggregatorType.ITEM, AggregatorState.STARTING, statusInfo);
		service = new AggregatorStatusMvcAdapter(status);
	}

	@Test
	public void testGetters() {
		assertEquals("boom", service.getImplCode());
		assertEquals("M15", service.getInterval());
		assertEquals(AggregatorType.ITEM, service.getType());
		assertEquals(AggregatorState.STARTING, service.getState());
		assertSame(statusInfo, service.getStatusInfo());
	}

}
