package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;
import static ru.prolib.caelum.core.Interval.*;
import static org.easymock.EasyMock.isA;

import java.util.Arrays;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.easymock.Capture;
import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({ "rawtypes", "unchecked", "unlikely-arg-type" })
public class KafkaAggregatorEntryTest {
	IMocksControl control;
	KafkaStreams streamsMock1, streamsMock2;
	KafkaAggregatorDescr descr1, descr2;
	ru.prolib.caelum.core.HostInfo hostInfo1, hostInfo2;
	KafkaStreamsAvailability stateMock1, stateMock2;
	ReadOnlyWindowStore<String, KafkaTuple> storeMock;
	KafkaAggregatorEntry service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsMock1 = control.createMock(KafkaStreams.class);
		streamsMock2 = control.createMock(KafkaStreams.class);
		descr1 = new KafkaAggregatorDescr(ITEM, M5, "items1", "tuples1", "store1");
		descr2 = new KafkaAggregatorDescr(TUPLE, M1, "items2", "tuples2", "store2");
		hostInfo1 = new ru.prolib.caelum.core.HostInfo("foobar", 10023);
		hostInfo2 = new ru.prolib.caelum.core.HostInfo("lowpan", 44321);
		stateMock1 = control.createMock(KafkaStreamsAvailability.class);
		stateMock2 = control.createMock(KafkaStreamsAvailability.class);
		storeMock = control.createMock(ReadOnlyWindowStore.class);
		service = new KafkaAggregatorEntry(hostInfo1, descr1, streamsMock1, stateMock1);
	}
	
	@Test
	public void testGetters() {
		assertEquals(hostInfo1, service.getHostInfo());
		assertEquals(descr1, service.getDescriptor());
		assertEquals(streamsMock1, service.getStreams());
		assertEquals(stateMock1, service.getState());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaAggregatorEntry[")
				.append("hostInfo=").append(hostInfo1).append(",")
				.append("descr=").append(descr1).append(",")
				.append("streams=").append(streamsMock1).append(",")
				.append("state=").append(stateMock1)
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1917, 11)
				.append(hostInfo1)
				.append(descr1)
				.append(streamsMock1)
				.append(stateMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorEntry(hostInfo1, descr1, streamsMock1, stateMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorEntry(hostInfo2, descr1, streamsMock1, stateMock1)));
		assertFalse(service.equals(new KafkaAggregatorEntry(hostInfo1, descr2, streamsMock1, stateMock1)));
		assertFalse(service.equals(new KafkaAggregatorEntry(hostInfo1, descr1, streamsMock2, stateMock1)));
		assertFalse(service.equals(new KafkaAggregatorEntry(hostInfo1, descr1, streamsMock1, stateMock2)));
		assertFalse(service.equals(new KafkaAggregatorEntry(hostInfo2, descr2, streamsMock2, stateMock2)));
	}
	
	@Test
	public void testGetStore() {
		Capture<StoreQueryParameters> cap = newCapture();
		expect(streamsMock1.store(capture(cap))).andReturn(storeMock);
		control.replay();
		
		assertSame(storeMock, service.getStore());
		
		control.verify();
		StoreQueryParameters p = cap.getValue();
		assertEquals("store1", p.storeName());
		assertThat(p.queryableStoreType(), is(instanceOf(QueryableStoreTypes.WindowStoreType.class)));
	}
	
	@Test
	public void testGetStore_ThrowsIfNotExists() {
		expect(streamsMock1.store(anyObject())).andReturn(null);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStore());
		assertEquals("Store not available: store1", e.getMessage());
	}
	
	@Test
	public void testGetStore1_ShouldReturnStateStore() {
		Capture<StoreQueryParameters> cap = newCapture();
		expect(stateMock1.waitForChange(true, 2500L)).andReturn(true);
		expect(streamsMock1.store(capture(cap))).andReturn(storeMock);
		control.replay();
		
		assertSame(storeMock, service.getStore(2500L));
		
		control.verify();
		StoreQueryParameters p = cap.getValue();
		assertEquals("store1", p.storeName());
		assertThat(p.queryableStoreType(), is(instanceOf(QueryableStoreTypes.WindowStoreType.class)));
		assertTrue(p.staleStoresEnabled());
	}
	
	@Test
	public void testGetStore1_ShouldThrowsIfStoreNotExists() {
		expect(stateMock1.waitForChange(true, 500L)).andReturn(true);
		expect(streamsMock1.store(anyObject())).andReturn(null);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStore(500L));
		
		control.verify();
		assertEquals("Store not available: store1", e.getMessage());
	}
	
	@Test
	public void testGetStore1_ShouldThrowsInCaseOfTimeout() {
		expect(stateMock1.waitForChange(true, 350L)).andReturn(false);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStore(350L));
		
		control.verify();
		assertEquals("Timeout while awaiting store availability: store1", e.getMessage());
	}
	
	@Test
	public void testGetStoreInfo2_ShouldThrowTimeoutExceptionIfStateNotAvailable() {
		expect(stateMock1.waitForChange(true, 9500L)).andReturn(false);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStoreInfo("foo", 9500L));
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Timeout while awaiting store availability: store1")));
	}
	
	@Test
	public void testGetStoreInfo2_ShouldThrowsIllegalStateIfMetadataNotAvailable() {
		expect(stateMock1.waitForChange(true, 1200L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("bar"), isA(StringSerializer.class)))
			.andReturn(KeyQueryMetadata.NOT_AVAILABLE);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStoreInfo("bar", 1200L));
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Metadata not available: store=store1 key=bar")));
	}
	
	@Test
	public void testGetStoreInfo2_ShouldReturnStoreIfThisHostIsActive() throws Exception {
		expect(stateMock1.waitForChange(true, 1500L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("foo"), isA(StringSerializer.class)))
			.andReturn(new KeyQueryMetadata(new HostInfo("foobar", 10023),
				new HashSet<>(Arrays.asList(new HostInfo("host1", 9001), new HostInfo("host2", 9002))), 5));
		Capture<StoreQueryParameters> capSQP = newCapture();
		expect(streamsMock1.store(capture(capSQP))).andReturn(storeMock);
		control.replay();
		
		KafkaAggregatorStoreInfo actual = service.getStoreInfo("foo", 1500L);
		
		control.verify();
		assertEquals(new KafkaAggregatorStoreInfo(hostInfo1, storeMock), actual);
		StoreQueryParameters p = capSQP.getValue();
		assertEquals("store1", p.storeName());
		assertThat(p.queryableStoreType(), is(instanceOf(QueryableStoreTypes.WindowStoreType.class)));
		assertTrue(p.staleStoresEnabled());
	}
	
	@Test
	public void testGetStoreInfo2_ShouldThrowsIfThisHostIsActiveButStoreNotExists() {
		expect(stateMock1.waitForChange(true, 2750L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("bar"), isA(StringSerializer.class)))
			.andReturn(new KeyQueryMetadata(new HostInfo("foobar", 10023), new HashSet<>(), 5));
		expect(streamsMock1.store(anyObject())).andReturn(null);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStoreInfo("bar", 2750L));
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Store not available: store1")));
	}
	
	@Test
	public void testGetStoreInfo2_ShouldReturnStoreIfThisHostIsStandby() throws Exception {
		expect(stateMock1.waitForChange(true, 7500L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("buzz"), isA(StringSerializer.class)))
			.andReturn(new KeyQueryMetadata(new HostInfo("host1", 9001),
				new HashSet<>(Arrays.asList(new HostInfo("foobar", 10023), new HostInfo("host2", 9002))), 1));
		Capture<StoreQueryParameters> capSQP = newCapture();
		expect(streamsMock1.store(capture(capSQP))).andReturn(storeMock);
		control.replay();
		
		KafkaAggregatorStoreInfo actual = service.getStoreInfo("buzz", 7500L);
		
		control.verify();
		assertEquals(new KafkaAggregatorStoreInfo(hostInfo1, storeMock), actual);
		StoreQueryParameters p = capSQP.getValue();
		assertEquals("store1", p.storeName());
		assertThat(p.queryableStoreType(), is(instanceOf(QueryableStoreTypes.WindowStoreType.class)));
		assertTrue(p.staleStoresEnabled());
	}
	
	@Test
	public void testGetStoreInfo2_ShouldThrowsIfThisHostIsStandbyButStoreNotExists() {
		expect(stateMock1.waitForChange(true, 7500L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("buzz"), isA(StringSerializer.class)))
			.andReturn(new KeyQueryMetadata(new HostInfo("host1", 9001),
				new HashSet<>(Arrays.asList(new HostInfo("foobar", 10023))), 1));
		
		expect(streamsMock1.store(anyObject())).andReturn(null);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getStoreInfo("buzz", 7500L));
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Store not available: store1")));
	}
	
	@Test
	public void testGetStoreInfo2_ShouldReturnAnotherHostIfThisHostIsNotActiveAndNotStandby() throws Exception {
		expect(stateMock1.waitForChange(true, 12345L)).andReturn(true);
		expect(streamsMock1.queryMetadataForKey(eq("store1"), eq("umbar"), anyObject(Serializer.class)))
			.andReturn(new KeyQueryMetadata(new HostInfo("lowpan", 44321),
				new HashSet<>(Arrays.asList(new HostInfo("host2", 9002))), 8));
		control.replay();
		
		KafkaAggregatorStoreInfo actual = service.getStoreInfo("umbar", 12345L);
		
		control.verify();
		assertEquals(new KafkaAggregatorStoreInfo(hostInfo2), actual);
	}
	
	@Test
	public void testSetAvailable() {
		stateMock1.setAvailable(true);
		stateMock1.setAvailable(false);
		control.replay();
		
		service.setAvailable(true);
		service.setAvailable(false);
		
		control.verify();
	}
	
	@Test
	public void testIsAvailable() {
		expect(stateMock1.isAvailable()).andReturn(true).andReturn(false);
		control.replay();
		
		assertTrue(service.isAvailable());
		assertFalse(service.isAvailable());
		
		control.verify();
	}
	
	@Test
	public void testGetStreamsState() {
		expect(streamsMock1.state()).andReturn(State.CREATED).andReturn(State.PENDING_SHUTDOWN);
		control.replay();
		
		assertEquals(State.CREATED, service.getStreamsState());
		assertEquals(State.PENDING_SHUTDOWN, service.getStreamsState());
		
		control.verify();
	}

}
