package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.TupleType;

public class TupleIteratorTest {
	IMocksControl control;
	WindowStoreIterator<KafkaTuple> itMock1, itMock2;
	TupleIterator service;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		itMock1 = control.createMock(WindowStoreIterator.class);
		itMock2 = control.createMock(WindowStoreIterator.class);
		service = new TupleIterator("foo@bar", itMock1);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo@bar", service.getSymbol());
		assertEquals(itMock1, service.getSource());
	}
	
	@Test
	public void testHasNext() {
		expect(itMock1.hasNext()).andReturn(true).andReturn(false);
		control.replay();
		
		assertTrue(service.hasNext());
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testNext() {
		KafkaTuple kt = new KafkaTuple(1L, 5L, 1L, 3L, (byte)3, 1000L, null, (byte)2, TupleType.LONG_REGULAR);
		expect(itMock1.next()).andReturn(new KeyValue<>(15898293L, kt));
		control.replay();
		
		ITuple actual = service.next();
		
		control.verify();
		ITuple expected = new Tuple("foo@bar", 15898293L, kt);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testClose() throws Exception {
		itMock1.close();
		control.replay();
		
		service.close();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(901878297, 3091)
				.append("foo@bar")
				.append(itMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new TupleIterator("foo@bar", itMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new TupleIterator("boo@moo", itMock1)));
		assertFalse(service.equals(new TupleIterator("foo@bar", itMock2)));
		assertFalse(service.equals(new TupleIterator("boo@moo", itMock2)));
	}

}
