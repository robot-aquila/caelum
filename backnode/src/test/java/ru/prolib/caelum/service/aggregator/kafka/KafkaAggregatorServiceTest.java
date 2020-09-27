package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.lib.Interval.*;
import static ru.prolib.caelum.service.aggregator.AggregatorState.*;
import static ru.prolib.caelum.service.aggregator.AggregatorType.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.lib.kafka.KafkaTuple;
import ru.prolib.caelum.service.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.service.aggregator.AggregatedDataResponse;
import ru.prolib.caelum.service.aggregator.AggregatorStatus;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.kafka.utils.WindowStoreIteratorLimited;
import ru.prolib.caelum.service.aggregator.kafka.utils.WindowStoreIteratorStub;

@SuppressWarnings("unchecked")
public class KafkaAggregatorServiceTest {
	
	Instant T(long time) {
		return Instant.ofEpochMilli(time);
	}
	
	Instant T(String time_string) {
		return Instant.parse(time_string);
	}
	
	long TL(String time_string) {
		return T(time_string).toEpochMilli();
	}
	
	IMocksControl control;
	HostInfo hostInfo;
	Intervals intervals;
	KafkaStreamsRegistry registryMock;
	ReadOnlyWindowStore<String, KafkaTuple> storeMock;
	WindowStoreIterator<KafkaTuple> itMock, itStub;
	KafkaAggregatorEntry entryMock;
	IAggregator aggrMock1, aggrMock2;
	List<IAggregator> aggregators;
	KafkaAggregatorService service, mockedService;
	AggregatedDataRequest request;

	@Before
	public void setUp() throws Exception {
		request = null;
		control = createStrictControl();
		hostInfo = new HostInfo("192.168.99.100", 17520);
		intervals = new Intervals();
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		storeMock = control.createMock(ReadOnlyWindowStore.class);
		itMock = control.createMock(WindowStoreIterator.class);
		itStub = new WindowStoreIteratorStub<>();
		entryMock = control.createMock(KafkaAggregatorEntry.class);
		aggrMock1 = control.createMock(IAggregator.class);
		aggrMock2 = control.createMock(IAggregator.class);
		aggregators = Arrays.asList(aggrMock1, aggrMock2);
		service = new KafkaAggregatorService(intervals, registryMock, aggregators, 5000, false, 1700L);
		mockedService = partialMockBuilder(KafkaAggregatorService.class)
				.withConstructor(Intervals.class,KafkaStreamsRegistry.class,List.class,int.class,boolean.class,long.class)
				.withArgs(intervals, registryMock, aggregators, 500, true, 1700L)
				.addMockedMethod("createClear", IAggregator.class, boolean.class)
				.createMock();
	}
	
	@Test
	public void testGetters() {
		assertSame(intervals, service.getIntervals());
		assertSame(registryMock, service.getRegistry());
		assertEquals(Arrays.asList(aggrMock1, aggrMock2), service.getAggregatorList());
		assertEquals(5000, service.getMaxLimit());
		assertEquals(1700L, service.getTimeout());
	}
	
	@Test
	public void testFetch_ExistingAggregator() {
		expect(registryMock.getByInterval(M5)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z"))).andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:15:00Z").toEpochMilli(), T("2020-07-03T12:00:00Z").toEpochMilli(), 1000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 1000L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_MaxLimitReached() {
		expect(registryMock.getByInterval(M5)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z"))).andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:15:00Z").toEpochMilli(), T("2020-07-03T12:00:00Z").toEpochMilli(), 750000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 5000L))
			);
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testFetch_ExistingAggregator_TimeAlignment() {
		expect(registryMock.getByInterval(M5)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z")))
			.andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:18:26.091Z").toEpochMilli(), T("2020-07-03T11:57:08.007Z").toEpochMilli(), 2000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 2000L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_ShouldUseZeroTimeIfTimeFromWasNotSpecified() {
		expect(registryMock.getByInterval(M5)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", Instant.EPOCH, T(899999L))).andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5, null, 845297L, 2000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 2000L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_ShouldUseLongMaxIfTimeToWasNotSpecified() {
		expect(registryMock.getByInterval(M1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("car@man", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("car@man", T(60000L), T(Long.MAX_VALUE))).andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("car@man", M1, 115257L, null, 1500));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("car@man", new WindowStoreIteratorLimited<>(itMock, 1500L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_ShouldUseDefaultLimitIfLimitWasNotSpecified() {
		expect(registryMock.getByInterval(M1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("gap@map", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("gap@map", T(60000L), T(959999L))).andReturn(itMock);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("gap@map", M1, 115257L, 939713L, 3500));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("gap@map", new WindowStoreIteratorLimited<>(itMock, 3500L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_ShouldReturnHostInfoIfDataOnAnotherHost() {
		expect(registryMock.getByInterval(M1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("gap@map", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo));
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("gap@map", M1, null, null, null));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T12:00:00Z"), T("2020-07-03T23:59:59.999Z"))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T12:00:00Z").toEpochMilli(), T("2020-07-04T00:00:00Z").toEpochMilli(), 5000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 5000L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_MaxLimitReached() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T12:00:00Z"), T("2020-07-03T23:59:59.999Z"))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T12:00:00Z").toEpochMilli(), T("2020-07-04T00:00:00Z").toEpochMilli(), 25000));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 5000L))
			);
		assertEquals(expected, actual);
	}

	@Test
	public void testFetch_AggregateOnFly_TimeAlignment() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T08:00:00Z"), T("2020-07-03T12:59:59.999Z"))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T08:13:51.091Z").toEpochMilli(), T("2020-07-03T12:49:02.574Z").toEpochMilli(), 700));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 700L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_ShouldUseZeroTimeIfTimeFromWasNotSpecified() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", Instant.EPOCH, T("2020-07-03T12:59:59.999Z"))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1, null, TL("2020-07-03T12:49:02.574Z"), 700));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 700L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_ShouldUseLongMaxIfTimeToWasNotSpecified() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", T("2020-07-03T08:00:00Z"), T(Long.MAX_VALUE))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1, TL("2020-07-03T08:13:51.091Z"), null, 700));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 700L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_ShouldUseDefaultLimitIfLimitWasNotSpecified() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo, storeMock));
		expect(storeMock.fetch("foo@bar", Instant.EPOCH, T("2020-07-03T12:59:59.999Z"))).andReturn(itStub);
		control.replay();
		AggregatedDataResponse actual, expected;
		
		request = new AggregatedDataRequest("foo@bar", H1, 0L, TL("2020-07-03T12:49:02.574Z"), null);
		actual = service.fetch(request);
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo,
				new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
					new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 5000L))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_ShouldReturnHostInfoIfDataOnAnotherHost() {
		expect(registryMock.getByInterval(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStoreInfo("foo@bar", 1700L)).andReturn(new KafkaAggregatorStoreInfo(hostInfo));
		control.replay();
		AggregatedDataResponse actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1, null, null, null));
		
		control.verify();
		expected = new AggregatedDataResponse(hostInfo);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testCreateClear() throws Exception {
		aggrMock1.clear(true);
		control.replay();
		
		CompletableFuture<Void> actual = service.createClear(aggrMock1, true);
		
		assertNotNull(actual);
		actual.get(1, TimeUnit.SECONDS);
		control.verify();
	}

	@Test
	public void testClear_ShouldUseFuturesIfParallelismIsEnabled() {
		CompletableFuture<Void> f1 = new CompletableFuture<>(), f2 = new CompletableFuture<>();
		expect(mockedService.createClear(aggrMock1, false)).andReturn(f1);
		expect(mockedService.createClear(aggrMock2, false)).andReturn(f2);
		replay(mockedService);
		control.replay();
		new Thread(() -> { f1.complete(null); f2.complete(null); }).start();
		
		mockedService.clear(false);
		
		control.verify();
		verify(mockedService);
	}
	
	@Test
	public void testClear_ShouldClearAggregatorsConsecutivelyInCurrentThreadIfParallelismIsDisabled() {
		final Thread expected_thread = Thread.currentThread();
		aggrMock1.clear(true);
		expectLastCall().andAnswer(() -> { assertSame(expected_thread, Thread.currentThread()); return null; });
		aggrMock2.clear(true);
		expectLastCall().andAnswer(() -> { assertSame(expected_thread, Thread.currentThread()); return null; });
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}
	
	@Test
	public void testGetAggregationIntervals() {
		assertEquals(intervals.getIntervals(), service.getAggregationIntervals());
	}
	
	@Test
	public void testGetAggregatorStatus() {
		expect(aggrMock1.getStatus()).andReturn(new AggregatorStatus("AK", M1, ITEM, CREATED, null));
		expect(aggrMock2.getStatus()).andReturn(new AggregatorStatus("AK", H1, ITEM, CREATED, null));
		control.replay();
		
		List<AggregatorStatus> actual = service.getAggregatorStatus();
		
		control.verify();
		List<AggregatorStatus> expected = Arrays.asList(
				new AggregatorStatus("AK", M1, ITEM, CREATED, null),
				new AggregatorStatus("AK", H1, ITEM, CREATED, null)
			);
		assertEquals(expected, actual);
	}

}
