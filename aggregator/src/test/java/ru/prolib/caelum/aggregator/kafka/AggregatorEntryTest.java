package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;
import static ru.prolib.caelum.core.Period.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.easymock.Capture;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AggregatorEntryTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaStreams streamsMock1, streamsMock2;
	AggregatorDescr descr1, descr2;
	AggregatorEntry service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsMock1 = control.createMock(KafkaStreams.class);
		streamsMock2 = control.createMock(KafkaStreams.class);
		descr1 = new AggregatorDescr(ITEM, M5, "items1", "tuples1", "store1");
		descr2 = new AggregatorDescr(TUPLE, M1, "items2", "tuples2", "store2");
		service = new AggregatorEntry(descr1, streamsMock1);
	}
	
	@Test
	public void testGetters() {
		assertEquals(descr1, service.getDescriptor());
		assertEquals(streamsMock1, service.getStreams());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("AggregatorEntry[descr=AggregatorDescr[type=ITEM,period=M5")
				.append(",source=items1,target=tuples1,storeName=store1],streams=")
				.append(streamsMock1)
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1917, 11)
				.append(descr1)
				.append(streamsMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new AggregatorEntry(descr1, streamsMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new AggregatorEntry(descr2, streamsMock1)));
		assertFalse(service.equals(new AggregatorEntry(descr1, streamsMock2)));
		assertFalse(service.equals(new AggregatorEntry(descr2, streamsMock2)));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGetStore() {
		Capture<StoreQueryParameters> cap = newCapture();
		ReadOnlyWindowStore<Object, Object> storeMock = control.createMock(ReadOnlyWindowStore.class);
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
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Store not available: store1");
		expect(streamsMock1.store(anyObject())).andReturn(null);
		control.replay();
		
		service.getStore();
	}

}
