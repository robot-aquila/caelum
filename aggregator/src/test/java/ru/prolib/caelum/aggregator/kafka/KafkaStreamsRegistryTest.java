package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.core.Interval.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.streams.KafkaStreams;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.HostInfo;
import ru.prolib.caelum.core.Interval;
import ru.prolib.caelum.core.Intervals;

public class KafkaStreamsRegistryTest {
	IMocksControl control;
	KafkaAggregatorDescr descr1, descr2, descr3;
	KafkaStreams streamsMock1, streamsMock2, streamsMock3;
	Intervals intervalsMock;
	KafkaAggregatorEntry entryMock1, entryMock2, entryMock3;
	HostInfo hostInfo;
	Map<Interval, KafkaAggregatorEntry> byInterval;
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
		byInterval = new HashMap<>();
		intervalsMock = control.createMock(Intervals.class);
		entryMock1 = control.createMock(KafkaAggregatorEntry.class);
		entryMock2 = control.createMock(KafkaAggregatorEntry.class);
		entryMock3 = control.createMock(KafkaAggregatorEntry.class);
		hostInfo = new HostInfo("bambr", 1234);
		service = new KafkaStreamsRegistry(hostInfo, intervalsMock, byInterval);
		mockedService = partialMockBuilder(KafkaStreamsRegistry.class)
				.withConstructor(HostInfo.class, Intervals.class, Map.class)
				.withArgs(hostInfo, intervalsMock, byInterval)
				.addMockedMethod("createEntry")
				.createMock();
	}
	
	@Test
	public void testCtor3() {
		assertEquals(hostInfo, service.getHostInfo());
		assertSame(intervalsMock, service.getIntervals());
		assertSame(byInterval, service.getEntryByIntervalMap());
	}
	
	@Test
	public void testCtor2() {
		service = new KafkaStreamsRegistry(hostInfo, intervalsMock);
		assertEquals(hostInfo, service.getHostInfo());
		assertSame(intervalsMock, service.getIntervals());
		assertNotNull(service.getEntryByIntervalMap());
		assertThat(service.getEntryByIntervalMap(), is(instanceOf(ConcurrentHashMap.class)));
	}
	
	@Test
	public void testCreateEntry() {
		KafkaAggregatorEntry actual = service.createEntry(descr1, streamsMock1);
		
		assertNotNull(actual);
		assertEquals(hostInfo, actual.getHostInfo());
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
		assertEquals(entryMock1, byInterval.get(M1));
		assertEquals(entryMock2, byInterval.get(M5));
	}
	
	@Test
	public void testGetByInterval() {
		byInterval.put(M1, entryMock1);
		byInterval.put(M5, entryMock2);
		byInterval.put(H1, entryMock3);
		
		assertEquals(entryMock1, service.getByInterval(M1));
		assertEquals(entryMock2, service.getByInterval(M5));
		assertEquals(entryMock3, service.getByInterval(H1));
		assertNull(service.getByInterval(M10));
		assertNull(service.getByInterval(H4));
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly() {
		byInterval.put(M1, entryMock1);
		byInterval.put(M5, entryMock2);
		byInterval.put(H1, entryMock3);
		expect(intervalsMock.getSmallerIntervalsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		assertEquals(entryMock3, service.findSuitableAggregatorToRebuildOnFly(D1));
		
		control.verify();
	}
	
	@Test
	public void testFindSuitableAggregatorToRebuildOnFly_ThrowsIfNoSuitableAggregator() {
		expect(intervalsMock.getSmallerIntervalsThatCanFill(D1)).andReturn(Arrays.asList(H1, M10, M5, M1));
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> service.findSuitableAggregatorToRebuildOnFly(D1));
		assertEquals("No suitable aggregator was found to rebuild: D1", e.getMessage());
	}
	
	@Test
	public void testDeregister() {
		byInterval.put(M1, entryMock1);
		byInterval.put(M5, entryMock2);
		control.replay();
		
		service.deregister(descr1);
		assertNull(byInterval.get(M1));
		
		service.deregister(descr2);
		assertNull(byInterval.get(M5));
		
		service.deregister(descr3);
		assertNull(byInterval.get(M10));
		
		control.verify();
		assertEquals(0, byInterval.size());
	}
	
	@Test
	public void testSetStreamsAvailable() {
		byInterval.put(M1, entryMock1);
		byInterval.put(M5, entryMock2);
		entryMock1.setAvailable(true);
		entryMock2.setAvailable(false);
		control.replay();
		
		service.setAvailability(descr1, true);
		service.setAvailability(descr2, false);
		
		control.verify();
	}

}
