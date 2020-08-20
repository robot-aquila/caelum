package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.HostInfo;

@SuppressWarnings("unchecked")
public class KafkaAggregatorStoreInfoTest {
	IMocksControl control;
	ReadOnlyWindowStore<String, KafkaTuple> storeMock1, storeMock2;
	HostInfo hostInfo1, hostInfo2;
	KafkaAggregatorStoreInfo service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		storeMock1 = control.createMock(ReadOnlyWindowStore.class);
		storeMock2 = control.createMock(ReadOnlyWindowStore.class);
		hostInfo1 = new HostInfo("zombie", 8832);
		hostInfo2 = new HostInfo("bakhta", 9614);
		service = new KafkaAggregatorStoreInfo(hostInfo1, storeMock1);
	}
	
	@Test
	public void testCtor2() {
		assertFalse(service.askAnotherHost());
		assertEquals(hostInfo1, service.getHostInfo());
		assertSame(storeMock1, service.getStore());
	}
	
	@Test
	public void testCtor1() {
		service = new KafkaAggregatorStoreInfo(hostInfo2);
		assertTrue(service.askAnotherHost());
		assertEquals(hostInfo2, service.getHostInfo());
		assertNull(service.getStore());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(new KafkaAggregatorStoreInfo(hostInfo2).equals(new KafkaAggregatorStoreInfo(hostInfo2)));
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorStoreInfo(hostInfo1, storeMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(new KafkaAggregatorStoreInfo(hostInfo2).equals(new KafkaAggregatorStoreInfo(hostInfo1)));
		assertFalse(service.equals(new KafkaAggregatorStoreInfo(hostInfo2, storeMock1)));
		assertFalse(service.equals(new KafkaAggregatorStoreInfo(hostInfo1, storeMock2)));
		assertFalse(service.equals(new KafkaAggregatorStoreInfo(hostInfo2, storeMock2)));
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(7231121, 983)
				.append(false)
				.append(hostInfo1)
				.append(storeMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaAggregatorStoreInfo[")
				.append("askAnotherHost=false,")
				.append("hostInfo=").append(hostInfo1).append(",")
				.append("store=").append(storeMock1)
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}

}
