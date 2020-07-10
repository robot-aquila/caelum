package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import static org.easymock.EasyMock.*;

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
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.SymbolListRequest;

@SuppressWarnings("unchecked")
public class FDBTransactionListSymbolsTest {
	static Subspace space = new Subspace(Tuple.from("glx"));
	
	IMocksControl control;
	FDBSchema schema, schemaMock;
	Transaction trMock;
	AsyncIterable<KeyValue> iterableMock;
	FDBTransactionListSymbols service;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		trMock = control.createMock(Transaction.class);
		iterableMock = control.createMock(AsyncIterable.class);
		service = new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", null, 500), 1000);
	}
	
	@Test
	public void testApply_MaxLimit() {
		Range range_cat = space.get(Tuple.from(0x02, "cat1")).range(); 
		expect(trMock.getRange(aryEq(range_cat.begin), aryEq(range_cat.end), eq(100))).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@bambr")).pack(), Tuple.from(true).pack())
			))));
		control.replay();
		
		service = new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", null, 500), 100);
		ICloseableIterator<String> actual = service.apply(trMock);
		
		control.verify();
		ICloseableIterator<String> expected = new IteratorStub<>(Arrays.asList("cat1@bambr"));
		assertEquals(expected, actual);
		
	}
	
	@Test
	public void testApply_WoAfterSymbol() {
		Range range_cat = space.get(Tuple.from(0x02, "cat1")).range(); 
		expect(trMock.getRange(aryEq(range_cat.begin), aryEq(range_cat.end), eq(500))).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@bambr")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@chuwe")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@garbo")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@alpha")).pack(), Tuple.from(true).pack())
			))));
		control.replay();
		
		ICloseableIterator<String> actual = service.apply(trMock);
		
		control.verify();
		IteratorStub<String> expected =
				new IteratorStub<>(Arrays.asList("cat1@bambr", "cat1@chuwe", "cat1@garbo", "cat1@alpha"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testApply_WithExistingAfterSymbol() {
		Range range_cat = space.get(Tuple.from(0x02, "cat1")).range(); 
		expect(trMock.getRange(
				aryEq(space.get(Tuple.from(0x02, "cat1", "cat1@kambo")).pack()),
				aryEq(range_cat.end),
				eq(501)
			)).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@kambo")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@chuwe")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@garbo")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@alpha")).pack(), Tuple.from(true).pack())
			))));
		control.replay();
		
		service = new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", "cat1@kambo", 500), 1000);
		ICloseableIterator<String> actual = service.apply(trMock);
		
		control.verify();
		IteratorStub<String> expected = new IteratorStub<>(Arrays.asList("cat1@chuwe", "cat1@garbo", "cat1@alpha"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testApply_WithNonExistingAfterSymbol() {
		Range range_cat = space.get(Tuple.from(0x02, "cat1")).range(); 
		expect(trMock.getRange(
				aryEq(space.get(Tuple.from(0x02, "cat1", "cat1@kambo")).pack()),
				aryEq(range_cat.end),
				eq(4)
			)).andReturn(iterableMock);
		expect(iterableMock.iterator()).andReturn(new AsyncIteratorStub<>(new ArrayList<>(Arrays.asList(
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@bambr")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@chuwe")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@garbo")).pack(), Tuple.from(true).pack()),
				new KeyValue(space.get(Tuple.from(0x02, "cat1", "cat1@alpha")).pack(), Tuple.from(true).pack())
			))));
		control.replay();
		
		service = new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", "cat1@kambo", 3), 1000);
		ICloseableIterator<String> actual = service.apply(trMock);

		control.verify();
		IteratorStub<String> expected = new IteratorStub<>(Arrays.asList("cat1@bambr", "cat1@chuwe", "cat1@garbo"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1209865, 51)
				.append(schema)
				.append(new SymbolListRequest("cat1", null, 500))
				.append(1000)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", null, 500), 1000)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionListSymbols(schemaMock, new SymbolListRequest("cat1", null, 500), 1000)));
		assertFalse(service.equals(new FDBTransactionListSymbols(schema, new SymbolListRequest("boom", null, 200), 1000)));
		assertFalse(service.equals(new FDBTransactionListSymbols(schema, new SymbolListRequest("cat1", null, 500), 2000)));
		assertFalse(service.equals(new FDBTransactionListSymbols(schemaMock, new SymbolListRequest("boom", null, 200), 2000)));
	}

}
