package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.core.Period.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class AggregatorRegistryTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	AggregatorDescr descr1, descr2, descr3;
	KafkaStreams streamsMock1, streamsMock2, streamsMock3;
	Periods periodsMock;
	Map<Period, AggregatorEntry> byPeriod;
	AggregatorRegistry service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		descr1 = new AggregatorDescr(ITEM, M1, "foo", "bar", "buzz");
		descr2 = new AggregatorDescr(TUPLE, M5, "zulu", "charlie", "kappa");
		descr3 = new AggregatorDescr(TUPLE_ONFLY, M10, "boo", "test", "best");
		streamsMock1 = control.createMock(KafkaStreams.class);
		streamsMock2 = control.createMock(KafkaStreams.class);
		streamsMock3 = control.createMock(KafkaStreams.class);
		byPeriod = new HashMap<>();
		periodsMock = control.createMock(Periods.class);
		service = new AggregatorRegistry(periodsMock, byPeriod);
	}
	
	@Test
	public void testCtor2() {
		assertSame(periodsMock, service.getPeriods());
		assertSame(byPeriod, service.getEntryByPeriodMap());
	}
	
	@Test
	public void testCtor1() {
		service = new AggregatorRegistry(periodsMock);
		assertSame(periodsMock, service.getPeriods());
		assertNotNull(service.getEntryByPeriodMap());
		assertThat(service.getEntryByPeriodMap(), is(instanceOf(ConcurrentHashMap.class)));
	}
	
	@Test
	public void testCtor0() {
		service = new AggregatorRegistry();
		assertSame(Periods.getInstance(), service.getPeriods());
		assertNotNull(service.getEntryByPeriodMap());
		assertThat(service.getEntryByPeriodMap(), is(instanceOf(ConcurrentHashMap.class)));
	}
	
	@Test
	public void testRegister_ThrowsIfTypeNotAllowed() {
		eex.expect(IllegalArgumentException.class);
		eex.expectMessage("Aggregator of type is not allowed to register: TUPLE_ONFLY");
		
		service.register(descr3, streamsMock3);
	}
	
	@Test
	public void testRegister() {
		service.register(descr1, streamsMock1);
		service.register(descr2, streamsMock2);
		
		assertEquals(new AggregatorEntry(descr1, streamsMock1), byPeriod.get(M1));
		assertEquals(new AggregatorEntry(descr2, streamsMock2), byPeriod.get(M5));
	}
	
	@Test
	public void testGetByPeriod() {
		byPeriod.put(M1, new AggregatorEntry(descr1, streamsMock1));
		byPeriod.put(M5, new AggregatorEntry(descr2, streamsMock2));
		
		assertEquals(new AggregatorEntry(descr1, streamsMock1), service.getByPeriod(M1));
		assertEquals(new AggregatorEntry(descr2, streamsMock2), service.getByPeriod(M5));
		assertNull(service.getByPeriod(M10));
		assertNull(service.getByPeriod(H4));
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly() {
		byPeriod.put(M1, new AggregatorEntry(descr1, streamsMock1));
		byPeriod.put(M5, new AggregatorEntry(descr2, streamsMock2));
		expect(periodsMock.getSmallerPeriodsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		assertEquals(new AggregatorEntry(descr2, streamsMock2), service.findSuitableAggregatorToRebuildOnFly(D1));
		
		control.verify();
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly_ThrowsIfNoSuitableAggregator() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("No suitable aggregator was found to rebuild: D1");
		expect(periodsMock.getSmallerPeriodsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		service.findSuitableAggregatorToRebuildOnFly(D1);
	}

}
