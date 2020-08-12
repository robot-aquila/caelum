package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
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
import org.junit.Test;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;

public class KafkaStreamsRegistryTest {
	IMocksControl control;
	KafkaAggregatorDescr descr1, descr2, descr3;
	KafkaStreams streamsMock1, streamsMock2, streamsMock3;
	Periods periodsMock;
	KafkaAggregatorEntry entryMock1, entryMock2, entryMock3;
	Map<Period, KafkaAggregatorEntry> byPeriod;
	KafkaStreamsRegistry service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		descr1 = new KafkaAggregatorDescr(ITEM, M1, "foo", "bar", "buzz");
		descr2 = new KafkaAggregatorDescr(TUPLE, M5, "zulu", "charlie", "kappa");
		descr3 = new KafkaAggregatorDescr(TUPLE_ONFLY, M10, "boo", "test", "best");
		streamsMock1 = control.createMock(KafkaStreams.class);
		streamsMock2 = control.createMock(KafkaStreams.class);
		streamsMock3 = control.createMock(KafkaStreams.class);
		byPeriod = new HashMap<>();
		periodsMock = control.createMock(Periods.class);
		entryMock1 = control.createMock(KafkaAggregatorEntry.class);
		entryMock2 = control.createMock(KafkaAggregatorEntry.class);
		entryMock3 = control.createMock(KafkaAggregatorEntry.class);
		service = new KafkaStreamsRegistry(periodsMock, byPeriod);
		mockedService = partialMockBuilder(KafkaStreamsRegistry.class)
				.withConstructor(Periods.class, Map.class)
				.withArgs(periodsMock, byPeriod)
				.addMockedMethod("createEntry")
				.createMock();
	}
	
	@Test
	public void testCtor2() {
		assertSame(periodsMock, service.getPeriods());
		assertSame(byPeriod, service.getEntryByPeriodMap());
	}
	
	@Test
	public void testCtor1() {
		service = new KafkaStreamsRegistry(periodsMock);
		assertSame(periodsMock, service.getPeriods());
		assertNotNull(service.getEntryByPeriodMap());
		assertThat(service.getEntryByPeriodMap(), is(instanceOf(ConcurrentHashMap.class)));
	}
	
	@Test
	public void testCreateEntry() {
		KafkaAggregatorEntry actual = service.createEntry(descr1, streamsMock1);
		
		assertNotNull(actual);
		assertEquals(descr1, actual.getDescriptor());
		assertEquals(streamsMock1, actual.getStreams());
		assertNotNull(actual.getState());
	}
	
	@Test
	public void testRegister_ThrowsIfTypeNotAllowed() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> service.register(descr3, streamsMock3));
		assertEquals("Aggregator of type is not allowed to register: TUPLE_ONFLY", e.getMessage());
	}
	
	@Test
	public void testRegister() {
		expect(mockedService.createEntry(same(descr1), same(streamsMock1))).andReturn(entryMock1);
		expect(mockedService.createEntry(eq(descr2), same(streamsMock2))).andReturn(entryMock2);
		control.replay();
		replay(mockedService);
		
		mockedService.register(descr1, streamsMock1);
		mockedService.register(descr2, streamsMock2);
		
		verify(mockedService);
		control.verify();
		assertEquals(entryMock1, byPeriod.get(M1));
		assertEquals(entryMock2, byPeriod.get(M5));
	}
	
	@Test
	public void testGetByPeriod() {
		byPeriod.put(M1, entryMock1);
		byPeriod.put(M5, entryMock2);
		byPeriod.put(H1, entryMock3);
		
		assertEquals(entryMock1, service.getByPeriod(M1));
		assertEquals(entryMock2, service.getByPeriod(M5));
		assertEquals(entryMock3, service.getByPeriod(H1));
		assertNull(service.getByPeriod(M10));
		assertNull(service.getByPeriod(H4));
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly() {
		byPeriod.put(M1, entryMock1);
		byPeriod.put(M5, entryMock2);
		byPeriod.put(H1, entryMock3);
		expect(periodsMock.getSmallerPeriodsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		assertEquals(entryMock3, service.findSuitableAggregatorToRebuildOnFly(D1));
		
		control.verify();
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly_ThrowsIfNoSuitableAggregator() {
		expect(periodsMock.getSmallerPeriodsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> service.findSuitableAggregatorToRebuildOnFly(D1));
		assertEquals("No suitable aggregator was found to rebuild: D1", e.getMessage());
	}
	
	@Test
	public void testDeregister() {
		byPeriod.put(M1, entryMock1);
		byPeriod.put(M5, entryMock2);
		control.replay();
		
		service.deregister(descr1);
		assertNull(byPeriod.get(M1));
		
		service.deregister(descr2);
		assertNull(byPeriod.get(M5));
		
		service.deregister(descr3);
		assertNull(byPeriod.get(M10));
		
		control.verify();
		assertEquals(0, byPeriod.size());
	}
	
	@Test
	public void testSetStreamsAvailable() {
		byPeriod.put(M1, entryMock1);
		byPeriod.put(M5, entryMock2);
		entryMock1.setAvailable(true);
		entryMock2.setAvailable(false);
		control.replay();
		
		service.setAvailability(descr1, true);
		service.setAvailability(descr2, false);
		
		control.verify();
	}

}
