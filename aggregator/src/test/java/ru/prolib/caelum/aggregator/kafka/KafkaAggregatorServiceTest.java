package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.core.Period.*;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.kafka.utils.WindowStoreIteratorLimited;
import ru.prolib.caelum.aggregator.kafka.utils.WindowStoreIteratorStub;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.Periods;

@SuppressWarnings("unchecked")
public class KafkaAggregatorServiceTest {
	
	Instant T(long time) {
		return Instant.ofEpochMilli(time);
	}
	
	Instant T(String time_string) {
		return Instant.parse(time_string);
	}
	
	IMocksControl control;
	Periods periods;
	KafkaAggregatorRegistry registryMock;
	ReadOnlyWindowStore<String, KafkaTuple> storeMock;
	WindowStoreIterator<KafkaTuple> itMock, itStub;
	KafkaAggregatorEntry entryMock;
	KafkaAggregatorService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		periods = Periods.getInstance();
		registryMock = control.createMock(KafkaAggregatorRegistry.class);
		storeMock = control.createMock(ReadOnlyWindowStore.class);
		itMock = control.createMock(WindowStoreIterator.class);
		itStub = new WindowStoreIteratorStub<>();
		entryMock = control.createMock(KafkaAggregatorEntry.class);
		service = new KafkaAggregatorService(periods, registryMock, 5000);
	}
	
	@Test
	public void testGetters() {
		assertSame(periods, service.getPeriods());
		assertSame(registryMock, service.getRegistry());
		assertEquals(5000, service.getMaxLimit());
	}
	
	@Test
	public void testFetch_ExistingAggregator() {
		expect(registryMock.getByPeriod(M5)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z"))).andReturn(itMock);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:15:00Z").toEpochMilli(), T("2020-07-03T12:00:00Z").toEpochMilli(), 1000));
		
		control.verify();
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 1000L));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_ExistingAggregator_MaxLimitReached() {
		expect(registryMock.getByPeriod(M5)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z"))).andReturn(itMock);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:15:00Z").toEpochMilli(), T("2020-07-03T12:00:00Z").toEpochMilli(), 750000));
		
		control.verify();
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 5000L));
		assertEquals(expected, actual);		
	}
	
	@Test
	public void testFetch_ExistingAggregator_TimeAlignment() {
		expect(registryMock.getByPeriod(M5)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T00:15:00Z"), T("2020-07-03T11:59:59.999Z")))
			.andReturn(itMock);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", M5,
				T("2020-07-03T00:18:26.091Z").toEpochMilli(), T("2020-07-03T11:57:08.007Z").toEpochMilli(), 2000));
		
		control.verify();
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(itMock, 2000L));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly() {
		expect(registryMock.getByPeriod(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T12:00:00Z"), T("2020-07-03T23:59:59.999Z"))).andReturn(itStub);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T12:00:00Z").toEpochMilli(), T("2020-07-04T00:00:00Z").toEpochMilli(), 5000));
		
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
				new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 5000L));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testFetch_AggregateOnFly_MaxLimitReached() {
		expect(registryMock.getByPeriod(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T12:00:00Z"), T("2020-07-03T23:59:59.999Z"))).andReturn(itStub);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T12:00:00Z").toEpochMilli(), T("2020-07-04T00:00:00Z").toEpochMilli(), 25000));
		
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
				new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 5000L));
		assertEquals(expected, actual);
	}

	@Test
	public void testFetch_AggregateOnFly_TimeAlignment() {
		expect(registryMock.getByPeriod(H1)).andReturn(null);
		expect(registryMock.findSuitableAggregatorToRebuildOnFly(H1)).andReturn(entryMock);
		expect(entryMock.getStore()).andReturn(storeMock);
		expect(storeMock.fetch("foo@bar", T("2020-07-03T08:00:00Z"), T("2020-07-03T12:59:59.999Z"))).andReturn(itStub);
		control.replay();
		ICloseableIterator<ITuple> actual, expected;
		
		actual = service.fetch(new AggregatedDataRequest("foo@bar", H1,
				T("2020-07-03T08:13:51.091Z").toEpochMilli(), T("2020-07-03T12:49:02.574Z").toEpochMilli(), 700));
		
		expected = new TupleIterator("foo@bar", new WindowStoreIteratorLimited<>(
				new KafkaTupleAggregateIterator(itStub, Duration.ofHours(1)), 700L));
		assertEquals(expected, actual);
	}

}
