package ru.prolib.caelum.service.symboldb.fdb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class FDBTransactionClearTest {
	static Subspace space = new Subspace(Tuple.from("karudo"));
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	FDBTransactionClear service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		service = new FDBTransactionClear(schema);
	}
	
	@Test
	public void testApply() {
		trMock.clear(space.range());
		control.replay();
		
		service.apply(trMock);
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(18210157, 349)
				.append(schema)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionClear(schema)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionClear(schemaMock)));
	}

}
