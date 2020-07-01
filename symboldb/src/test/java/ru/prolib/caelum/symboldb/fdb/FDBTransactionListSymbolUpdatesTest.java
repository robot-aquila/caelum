package ru.prolib.caelum.symboldb.fdb;

import static org.easymock.EasyMock.createStrictControl;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.CloseableIteratorStub;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.SymbolUpdate;

@SuppressWarnings("unchecked")
public class FDBTransactionListSymbolUpdatesTest {
	static Subspace space = new Subspace(Tuple.from("zoo"));
	
	static Map<Integer, String> toMap(Object ...args) {
		if ( args.length % 2 != 0 ) {
			throw new IllegalArgumentException();
		}
		Map<Integer, String> result = new LinkedHashMap<>();
		for ( int i = 0; i < args.length; i += 2 ) {
			result.put((Integer) args[i], (String) args[i + 1]);
		}
		return result;
	}
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	AsyncIterable<KeyValue> iterableMock;
	FDBTransactionListSymbolUpdates service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		iterableMock = control.createMock(AsyncIterable.class);
		service = new FDBTransactionListSymbolUpdates(schema, "barak");
	}
	
	@Test
	public void testApply() {
		AsyncIteratorStub<KeyValue> it = new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x03, "barak", 17829L)).pack(),
					Tuple.from(5, "goo", 6, "moo", 7, "gap").pack()),
				new KeyValue(space.get(Tuple.from(0x03, "barak", 17950L)).pack(),
					Tuple.from(1, "foo", 2, "bar", 3, "zoo").pack()),
				new KeyValue(space.get(Tuple.from(0x03, "barak", 17980L)).pack(),
					Tuple.from(5, "ups").pack())
			)));
		expect(trMock.getRange(space.get(Tuple.from(0x03, "barak")).range())).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(it);
		control.replay();
		
		ICloseableIterator<SymbolUpdate> actual = service.apply(trMock);
		
		control.verify();
		ICloseableIterator<SymbolUpdate> expected = new CloseableIteratorStub<>(new ArrayList<>(Arrays.asList(
				new SymbolUpdate("barak", 17829L, toMap(5, "goo", 6, "moo", 7, "gap")),
				new SymbolUpdate("barak", 17950L, toMap(1, "foo", 2, "bar", 3, "zoo")),
				new SymbolUpdate("barak", 17980L, toMap(5, "ups"))
			)));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1699771, 89)
				.append(schema)
				.append("barak")
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionListSymbolUpdates(schema, "barak")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionListSymbolUpdates(schemaMock, "barak")));
		assertFalse(service.equals(new FDBTransactionListSymbolUpdates(schema, "durak")));
		assertFalse(service.equals(new FDBTransactionListSymbolUpdates(schemaMock, "durak")));
	}

}
