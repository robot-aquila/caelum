package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.kafka.KafkaTuple;
import ru.prolib.caelum.aggregator.kafka.Tuple;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.TupleType;

public class TupleMvcAdapterIteratorTest {
	
	static KafkaTuple KT(long open, long high, long low, long close, long volume) {
		return new KafkaTuple(open, high, low, close, (byte)3, volume, null, (byte)0, TupleType.LONG_REGULAR);
	}
	
	static ITuple T(long time, long open, long high, long low, long close, long volume) {
		return new Tuple("foo@bar", time, KT(open, high, low, close, volume));
	}
	
	static TupleMvcAdapter KVA(long time, long open, long high, long low, long close, long volume) {
		return new TupleMvcAdapter(T(time, open, high, low, close, volume));
	}
	
	IteratorStub<ITuple> iterator;
	TupleMvcAdapterIterator service;

	@Before
	public void setUp() throws Exception {
		iterator = new IteratorStub<>(Arrays.asList(
				T(1000L, 25, 27, 23, 28, 100),
				T(2000L, 28, 30, 24, 27, 200),
				T(3000L, 31, 31, 28, 30, 150),
				T(4000L, 33, 35, 31, 35, 300)
			), true);
		service = new TupleMvcAdapterIterator(iterator);
	}
	
	@Test
	public void testIterate() {
		assertTrue(service.hasNext());
		assertEquals(KVA(1000L, 25, 27, 23, 28, 100), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KVA(2000L, 28, 30, 24, 27, 200), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KVA(3000L, 31, 31, 28, 30, 150), service.next());
		
		assertTrue(service.hasNext());
		assertEquals(KVA(4000L, 33, 35, 31, 35, 300), service.next());
		
		assertFalse(service.hasNext());
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
}
