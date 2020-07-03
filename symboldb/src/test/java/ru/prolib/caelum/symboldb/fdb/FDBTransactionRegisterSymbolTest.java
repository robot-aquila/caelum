package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Arrays;

import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterSymbolTest {
	static Subspace space = new Subspace(Tuple.from("foo"));
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	ICategoryExtractor catExtMock1, catExtMock2;
	Transaction trMock;
	FDBTransactionRegisterSymbol service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		catExtMock1 = control.createMock(ICategoryExtractor.class);
		catExtMock2 = control.createMock(ICategoryExtractor.class);
		trMock = control.createMock(Transaction.class);
		service = new FDBTransactionRegisterSymbol(schema, catExtMock1, "lumbari");
	}
	
	@Test
	public void testApply() {
		expect(catExtMock1.extract("lumbari")).andReturn(Arrays.asList("cat1", "cat2", "cat3"));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat1")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat1", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat2")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat2", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat3")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat3", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		control.replay();
		
		assertNull(service.apply(trMock));

		control.verify();
	}

	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115389, 27)
				.append(schema)
				.append(catExtMock1)
				.append("lumbari")
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionRegisterSymbol(schema, catExtMock1, "lumbari")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schemaMock, catExtMock1, "lumbari")));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schema, catExtMock2, "lumbari")));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schema, catExtMock1, "bamboor")));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schemaMock, catExtMock2, "bamboor")));
	}

}
