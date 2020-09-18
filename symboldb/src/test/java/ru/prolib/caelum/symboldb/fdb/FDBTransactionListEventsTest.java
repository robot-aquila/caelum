package ru.prolib.caelum.symboldb.fdb;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.EventsBuilder;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.EventListRequest;

@SuppressWarnings("unchecked")
public class FDBTransactionListEventsTest {
	static Subspace space = new Subspace(Tuple.from("zoo"));
	
	static KeyValue kv(String symbol, long time, int event_id, String event_data) {
		return new KeyValue(space.get(Tuple.from(0x03, symbol, time, event_id)).pack(), event_data.getBytes());
	}
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	AsyncIterable<KeyValue> iterableMock;
	EventListRequest request1, request2;
	FDBTransactionListEvents service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		iterableMock = control.createMock(AsyncIterable.class);
		request1 = new EventListRequest("barak", null, null, 7);
		request2 = new EventListRequest("kaboom", 15998827L, 17091812L, 100);
		service = new FDBTransactionListEvents(schema, request1, 7);
	}
	
	@Test
	public void testApply_ShouldUseDefaultParamsIfNotOverriden() {
		AsyncIteratorStub<KeyValue> it = new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				kv("barak", 17829L, 5, "goo"),
				kv("barak", 17829L, 6, "moo"),
				kv("barak", 17829L, 7, "gap"),
				kv("barak", 17850L, 1, "foo"),
				kv("barak", 17850L, 2, "bar"),
				kv("barak", 17850L, 3, "zoo"),
				kv("barak", 17880L, 5, "ups")
			)));
		Range exp_rng = schema.getSpaceEvents("barak").range();
		expect(trMock.getRange(aryEq(exp_rng.begin), aryEq(exp_rng.end))).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(it);
		control.replay();
		
		ICloseableIterator<Events> actual = service.apply(trMock);
		
		control.verify();
		ICloseableIterator<Events> expected = new IteratorStub<>(Arrays.asList(
				new EventsBuilder()
					.withSymbol("barak")
					.withTime(17829L)
					.withEvent(5, "goo")
					.withEvent(6, "moo")
					.withEvent(7, "gap")
					.build(),
				new EventsBuilder()
					.withSymbol("barak")
					.withTime(17850L)
					.withEvent(1, "foo")
					.withEvent(2, "bar")
					.withEvent(3, "zoo")
					.build(),
				new EventsBuilder()
					.withSymbol("barak")
					.withTime(17880L)
					.withEvent(5, "ups")
					.build()
			), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testApply_ShouldUseRequestParams() {
		request1 = new EventListRequest("barak", 15340L, 18210L, 5);
		service = new FDBTransactionListEvents(schema, request1, 7);
		AsyncIteratorStub<KeyValue> it = new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				kv("barak", 17829L, 1, "goo"),
				kv("barak", 17830L, 1, "moo"),
				kv("barak", 17831L, 1, "gap"),
				kv("barak", 17832L, 1, "foo"),
				kv("barak", 17833L, 1, "bar"),
				kv("barak", 17834L, 1, "zoo"),
				kv("barak", 17835L, 1, "ups"),
				kv("barak", 17836L, 1, "boo")
			)));
		
		byte exp_beg[] = space.get(Tuple.from(0x03, "barak", 15340L)).getKey();
		byte exp_end[] = space.get(Tuple.from(0x03, "barak", 18210L)).getKey();
		expect(trMock.getRange(aryEq(exp_beg), aryEq(exp_end))).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(it);
		control.replay();
		
		ICloseableIterator<Events> actual = service.apply(trMock);
		
		control.verify();
		EventsBuilder builder = new EventsBuilder().withSymbol("barak");
		ICloseableIterator<Events> expected = new IteratorStub<>(Arrays.asList(
				builder.withTime(17829L).withEvent(1, "goo").build(),
				builder.withTime(17830L).withEvent(1, "moo").build(),
				builder.withTime(17831L).withEvent(1, "gap").build(),
				builder.withTime(17832L).withEvent(1, "foo").build(),
				builder.withTime(17833L).withEvent(1, "bar").build()
			), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testApply_ShouldForceMaxLimitIfRequestedLimitTooBig() {
		request1 = new EventListRequest("barak", null, null, 25);
		service = new FDBTransactionListEvents(schema, request1, 7);
		AsyncIteratorStub<KeyValue> it = new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				kv("barak", 17829L, 1, "goo"),
				kv("barak", 17830L, 1, "moo"),
				kv("barak", 17831L, 1, "gap"),
				kv("barak", 17832L, 1, "foo"),
				kv("barak", 17833L, 1, "bar"),
				kv("barak", 17834L, 1, "zoo"),
				kv("barak", 17835L, 1, "ups"),
				kv("barak", 17836L, 1, "boo")
			)));
		Range exp_rng = schema.getSpaceEvents("barak").range();
		expect(trMock.getRange(aryEq(exp_rng.begin), aryEq(exp_rng.end))).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(it);
		control.replay();
		
		ICloseableIterator<Events> actual = service.apply(trMock);
		
		control.verify();
		EventsBuilder builder = new EventsBuilder().withSymbol("barak");
		ICloseableIterator<Events> expected = new IteratorStub<>(Arrays.asList(
				builder.withTime(17829L).withEvent(1, "goo").build(),
				builder.withTime(17830L).withEvent(1, "moo").build(),
				builder.withTime(17831L).withEvent(1, "gap").build(),
				builder.withTime(17832L).withEvent(1, "foo").build(),
				builder.withTime(17833L).withEvent(1, "bar").build(),
				builder.withTime(17834L).withEvent(1, "zoo").build(),
				builder.withTime(17835L).withEvent(1, "ups").build()
			), true);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1699771, 89)
				.append(schema)
				.append(request1)
				.append(7)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionListEvents(schema, request1, 7)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionListEvents(schemaMock, request1, 7)));
		assertFalse(service.equals(new FDBTransactionListEvents(schema, request2, 7)));
		assertFalse(service.equals(new FDBTransactionListEvents(schema, request1, 8)));
		assertFalse(service.equals(new FDBTransactionListEvents(schemaMock, request2, 8)));
	}

}
