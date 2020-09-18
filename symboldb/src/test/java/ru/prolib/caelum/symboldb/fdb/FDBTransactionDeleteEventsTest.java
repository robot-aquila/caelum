package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.lib.Events;

public class FDBTransactionDeleteEventsTest {
	static Subspace space = new Subspace(Tuple.from("bar"));
	
	static Map<Integer, String> toMap(Object ...args) {
		Map<Integer, String> result = new LinkedHashMap<>();
		if ( args.length % 2 != 0 ) {
			throw new IllegalArgumentException();
		}
		for ( int i = 0; i < args.length / 2; i ++ ) {
			result.put((int) args[i * 2], (String) args[i * 2 + 1]);
		}
		return result;
	}
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	Events events1, events2, events3;
	Collection<Events> list1, list2;
	FDBTransactionDeleteEvents service;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		events1 = new Events("foo@bar", 1728299L, toMap(1001, "420", 2005, "hello"));
		events2 = new Events("zoo@com", 1829203L, toMap(405, "tam-tam", 205, "2020-09-17", 108, "lorem dolorem"));
		events3 = new Events("foo@gap", 1588299L, toMap(5004, "tachi"));
		service = new FDBTransactionDeleteEvents(schema, list1 = Arrays.asList(events1, events2, events3));
		list2 = Arrays.asList(events3, events1);
	}
	
	@Test
	public void testApply() {
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "foo@bar", 1728299L, 1001)).pack()));
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "foo@bar", 1728299L, 2005)).pack()));
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "zoo@com", 1829203L,  405)).pack()));
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "zoo@com", 1829203L,  205)).pack()));
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "zoo@com", 1829203L,  108)).pack()));
		trMock.clear(aryEq(space.get(Tuple.from(0x03, "foo@gap", 1588299L, 5004)).pack()));
		control.replay();
		
		assertNull(service.apply(trMock));

		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(14492617, 75)
				.append(schema)
				.append(list1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionDeleteEvents(schema, list1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionDeleteEvents(schemaMock, list1)));
		assertFalse(service.equals(new FDBTransactionDeleteEvents(schema, list2)));
		assertFalse(service.equals(new FDBTransactionDeleteEvents(schemaMock, list2)));
	}

}
