package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterEventsTest {
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
	ICategoryExtractor catExt1, catExt2;
	Transaction trMock;
	Events events1, events2, events3;
	FDBTransactionRegisterEvents service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		schema = new FDBSchema(space);
		schemaMock = control.createMock(FDBSchema.class);
		catExt1 = new CommonCategoryExtractor();
		catExt2 = control.createMock(ICategoryExtractor.class);
		trMock = control.createMock(Transaction.class);
		events1 = new Events("foo@bar", 1728299L, toMap(1001, "420", 2005, "hello"));
		events2 = new Events("zoo@com", 1829203L, toMap(405, "tam-tam", 205, "2020-09-17", 108, "lorem dolorem"));
		events3 = new Events("foo@gap", 1588299L, toMap(5004, "tachi"));
		service = new FDBTransactionRegisterEvents(schema, catExt1,
				Arrays.asList(events1, events2, events3), new LinkedHashSet<>(), new LinkedHashMap<>());
	}
	
	@Test
	public void testApply() {
		Tuple x = null;
		byte tb[] = Tuple.from(true).pack();
		trMock.set(aryEq(space.get(Tuple.from(0x01, "foo")).pack()), aryEq(tb));
		trMock.set(aryEq(space.get(Tuple.from(0x01, "zoo")).pack()), aryEq(tb));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo", "foo@bar")).pack()), aryEq(tb));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "zoo", "zoo@com")).pack()), aryEq(tb));
		trMock.set(aryEq(space.get(Tuple.from(0x02, "foo", "foo@gap")).pack()), aryEq(tb));
		x = Tuple.from(0x03, "foo@bar", 1728299L);
		trMock.set(aryEq(space.get(x.add(1001)).pack()), aryEq("420".getBytes()));
		trMock.set(aryEq(space.get(x.add(2005)).pack()), aryEq("hello".getBytes()));
		x = Tuple.from(0x03, "zoo@com", 1829203L);
		trMock.set(aryEq(space.get(x.add(405)).pack()), aryEq("tam-tam".getBytes()));
		trMock.set(aryEq(space.get(x.add(205)).pack()), aryEq("2020-09-17".getBytes()));
		trMock.set(aryEq(space.get(x.add(108)).pack()), aryEq("lorem dolorem".getBytes()));
		x = Tuple.from(0x03, "foo@gap", 1588299L);
		trMock.set(aryEq(space.get(x.add(5004)).pack()), aryEq("tachi".getBytes()));
		control.replay();
		
		assertNull(service.apply(trMock));

		control.verify();
	}

	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(9000113, 307)
				.append(schema)
				.append(catExt1)
				.append(Arrays.asList(events1, events2, events3))
				.build();

		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		Collection<Events> list1 = Arrays.asList(events1, events2, events3);
		Collection<Events> list2 = Arrays.asList(events3, events1);
		expect(catExt2.extract(anyObject())).andStubReturn(Arrays.asList("", "cat1"));
		control.replay();
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBTransactionRegisterEvents(schema, catExt1, list1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBTransactionRegisterEvents(schemaMock, catExt1, list1)));
		assertFalse(service.equals(new FDBTransactionRegisterEvents(schema, catExt2, list1)));
		assertFalse(service.equals(new FDBTransactionRegisterEvents(schema, catExt1, list2)));
		assertFalse(service.equals(new FDBTransactionRegisterEvents(schemaMock, catExt2, list2)));
	}

}
