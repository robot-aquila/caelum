package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;

import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.ICategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBTransactionRegisterSymbolUpdateTest {
	static Subspace space = new Subspace(Tuple.from("bar"));
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	ICategoryExtractor catExtMock1, catExtMock2;
	Transaction trMock;
	SymbolUpdate update1, update2;
	FDBTransactionRegisterSymbolUpdate service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		catExtMock1 = control.createMock(ICategoryExtractor.class);
		catExtMock2 = control.createMock(ICategoryExtractor.class);
		trMock = control.createMock(Transaction.class);
		update1 = new SymbolUpdate("foo@bar", 1728299L, new HashMap<>());
		update2 = new SymbolUpdate("zoo@com", 1829203L, new HashMap<>());
		service = new FDBTransactionRegisterSymbolUpdate(schema, catExtMock1, update1);
	}
	
	@Test
	public void testApply() {
		expect(catExtMock1.extract("foo@bar")).andReturn(Arrays.asList("foo1", "foo2", "foo3"));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "foo1")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo1", "foo@bar")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "foo2")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo2", "foo@bar")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "foo3")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo3", "foo@bar")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x03, "foo@bar", 1728299L)).pack()), aryEq(new Tuple().pack()));
		control.replay();
		
		assertNull(service.apply(trMock));

		control.verify();
	}

	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(9000113, 307)
				.append(schema)
				.append(catExtMock1)
				.append(update1)
				.build();

		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionRegisterSymbolUpdate(schema, catExtMock1,
				new SymbolUpdate("foo@bar", 1728299L, new HashMap<>()))));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionRegisterSymbolUpdate(schemaMock, catExtMock1, update1)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbolUpdate(schema, catExtMock2, update1)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbolUpdate(schema, catExtMock1, update2)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbolUpdate(schemaMock, catExtMock2, update2)));
	}

}
