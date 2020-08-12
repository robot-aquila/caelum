package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterSymbolTest {
	static Subspace space = new Subspace(Tuple.from("foo"));
	
	static class TestCatExtractor implements ICategoryExtractor {

		@Override
		public Collection<String> extract(String symbol) {
			if ( "lumbari".equals(symbol) ) {
				return Arrays.asList("cat1", "cat2", "cat3");
			}
			return Arrays.asList();
		}
		
	}
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	ICategoryExtractor catExt1, catExt2;
	Transaction trMock;
	FDBTransactionRegisterSymbol service;
	List<String> symbols1, symbols2;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		catExt1 = CommonCategoryExtractor.getInstance();
		catExt2 = new TestCatExtractor();
		trMock = control.createMock(Transaction.class);
		symbols1 = Arrays.asList("foo@tulusa", "bob@barbi", "foo@lumbari", "bob@fake");
		symbols2 = Arrays.asList("gamma@foo", "gamma@bar");
		service = new FDBTransactionRegisterSymbol(schema, catExt1, symbols1);
	}
	
	@Test
	public void testApply_JustSingleSymbol() {
		service = new FDBTransactionRegisterSymbol(schema, new TestCatExtractor(), Arrays.asList("lumbari"));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat1")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat2")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "cat3")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat1", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat2", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "cat3", "lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		control.replay();
		
		assertNull(service.apply(trMock));

		control.verify();
	}
	
	@Test
	public void testApply_MultipleSymbols() {
		trMock.set(aryEq(space.get(Tuple.from(0x01, "foo")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "bob")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo", "foo@tulusa")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "bob", "bob@barbi")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo", "foo@lumbari")).pack()), aryEq(Tuple.from(true).pack()));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "bob", "bob@fake")).pack()), aryEq(Tuple.from(true).pack()));
		control.replay();
		
		assertNull(service.apply(trMock));
		
		control.verify();
	}

	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115389, 27)
				.append(schema)
				.append(catExt1)
				.append(symbols1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionRegisterSymbol(schema, catExt1, symbols1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schemaMock, catExt1, symbols1)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schema,     catExt2, symbols1)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schema,     catExt1, symbols2)));
		assertFalse(service.equals(new FDBTransactionRegisterSymbol(schemaMock, catExt1, symbols1)));
	}

}
