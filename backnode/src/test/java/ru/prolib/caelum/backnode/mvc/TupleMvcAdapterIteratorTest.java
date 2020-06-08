package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.WindowStoreIteratorStub;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.core.TupleType;

public class TupleMvcAdapterIteratorTest {
	
	static Tuple T(long open, long high, long low, long close, long volume) {
		return new Tuple(open, high, low, close, (byte)3, volume, null, (byte)0, TupleType.LONG_REGULAR);
	}
	
	static KeyValue<Long, Tuple> KV(long key, long open, long high, long low, long close, long volume) {
		return new KeyValue<>(key, T(open, high, low, close, volume));
	}
	
	static TupleMvcAdapter KVA(long key, long open, long high, long low, long close, long volume) {
		return new TupleMvcAdapter(key, T(open, high, low, close, volume));
	}
	
	@Rule
	public ExpectedException eex = ExpectedException.none();
	WindowStoreIteratorStub<Tuple> iterator;
	TupleMvcAdapterIterator service;

	@Before
	public void setUp() throws Exception {
		iterator = new WindowStoreIteratorStub<>(Arrays.asList(
				KV(1000L, 25, 27, 23, 28, 100),
				KV(2000L, 28, 30, 24, 27, 200),
				KV(3000L, 31, 31, 28, 30, 150),
				KV(4000L, 33, 35, 31, 35, 300)
			));
		service = new TupleMvcAdapterIterator(iterator);
	}
	
	@Test
	public void testIterate() {
		assertEquals(Long.valueOf(1000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(KVA(1000L, 25, 27, 23, 28, 100), service.next());
		
		assertEquals(Long.valueOf(2000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(KVA(2000L, 28, 30, 24, 27, 200), service.next());
		
		assertEquals(Long.valueOf(3000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(KVA(3000L, 31, 31, 28, 30, 150), service.next());
		
		assertEquals(Long.valueOf(4000L), service.peekNextKey());
		assertTrue(service.hasNext());
		assertEquals(KVA(4000L, 33, 35, 31, 35, 300), service.next());
		
		assertFalse(service.hasNext());
	}

	@Test
	public void testPeekNextKey_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		service.next();
		eex.expect(NoSuchElementException.class);
		
		service.peekNextKey();
	}
	
	@Test
	public void testPeekNextKey_ThrowsIfClosed() {
		service.close();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.peekNextKey();
	}
	
	@Test
	public void testNext_ThrowsIfNoMoreData() {
		service.next();
		service.next();
		service.next();
		service.next();
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		service.close();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.next();
	}

}
