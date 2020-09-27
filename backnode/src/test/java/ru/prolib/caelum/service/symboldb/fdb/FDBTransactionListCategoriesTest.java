package ru.prolib.caelum.service.symboldb.fdb;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.IteratorStub;

@SuppressWarnings("unchecked")
public class FDBTransactionListCategoriesTest {
	static Subspace space = new Subspace(Tuple.from("baa"));
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	AsyncIterable<KeyValue> iterableMock;
	FDBTransactionListCategories service;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		iterableMock = control.createMock(AsyncIterable.class);
		service = new FDBTransactionListCategories(schema);
	}
	
	@Test
	public void testApply() {
		AsyncIteratorStub<KeyValue> it = new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x01, "bumba")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x01, "gamba")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x01, "toper")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x01, "lisar")).pack(), Tuple.from(true).pack())
			)));
		expect(trMock.getRange(space.get(0x01).range())).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(it);
		control.replay();
		
		ICloseableIterator<String> actual = service.apply(trMock);
		
		control.verify();
		IteratorStub<String> expected = new IteratorStub<>(Arrays.asList("bumba", "gamba", "toper", "lisar"), true);
		assertEquals(expected, actual);
	}

	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(509727, 21)
				.append(schema)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionListCategories(schema)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionListCategories(schemaMock)));
	}

}
