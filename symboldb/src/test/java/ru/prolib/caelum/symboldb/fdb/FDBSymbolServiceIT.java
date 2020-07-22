package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSymbolServiceIT {
	static FDBTestHelper helper;
	static Database db;
	static Subspace space;
	static byte[] TB;
	
	@ClassRule public static Timeout globalTimeout = Timeout.millis(5000);
	
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
	
	static <T> List<T> toList(ICloseableIterator<T> it) throws Exception {
		List<T> result = new ArrayList<>();
		while ( it.hasNext() ) {
			result.add(it.next());
		}
		it.close();
		return result;
	}
		
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		helper = new FDBTestHelper("fdb.cluster");
		db = helper.getDB();
		space = helper.getTestSubspace();
		TB = Tuple.from(true).pack();
	}
	
	FDBSymbolService service;

	@Before
	public void setUp() throws Exception {
		service = new FDBSymbolService(CommonCategoryExtractor.getInstance(),
				new FDBSchema(helper.getTestSubspace()), 100);
		service.setDatabase(db);
	}
	
	@After
	public void tearDown() throws Exception {
		helper.clearTestSubspace();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		helper.close();
	}
	
	@Test
	public void testRegisterSymbol_S() {
		service.registerSymbol("tukana@batari");
		service.registerSymbol("kappa");
		
		db.run((t) -> {
			// categories
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "tukana")).pack()).join());
			// symbols
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "", "kappa")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "tukana", "tukana@batari")).pack()).join());
			return null;
		});
	}
	
	@Test
	public void testRegisterSymbol_L() {
		service.registerSymbol(Arrays.asList("tukana@batari", "kappa"));
		
		db.run((t) -> {
			// categories
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "tukana")).pack()).join());
			// symbols
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "", "kappa")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "tukana", "tukana@batari")).pack()).join());
			return null;
		});
	}
	
	@Test
	public void testRegisterSymbolUpdate() {
		service.registerSymbolUpdate(new SymbolUpdate("kap@mov", 15882689L, toMap(10, "foo", 11, "bar", 12, "buz")));
		service.registerSymbolUpdate(new SymbolUpdate("kap@alp", 16788992L, toMap(20, "alp", 30, "zip", 40, "gap")));
		service.registerSymbolUpdate(new SymbolUpdate("kap@alp", 16911504L, toMap(21, "boo", 22, "moo", 23, "goo")));
		service.registerSymbolUpdate(new SymbolUpdate("bar@gor", 17829914L, toMap(11, "zoo", 12, "ups", 13, "pop")));
		
		db.run((t) -> {
			// categories
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "kap")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "bar")).pack()).join());
			// symbols
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "kap", "kap@mov")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "kap", "kap@alp")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "bar", "bar@gor")).pack()).join());
			// updates
			assertArrayEquals(Tuple.from(10, "foo", 11, "bar", 12, "buz").pack(),
				t.get(space.get(Tuple.from(0x03, "kap@mov", 15882689L)).pack()).join());
			assertArrayEquals(Tuple.from(20, "alp", 30, "zip", 40, "gap").pack(),
					t.get(space.get(Tuple.from(0x03, "kap@alp", 16788992L)).pack()).join());
			assertArrayEquals(Tuple.from(21, "boo", 22, "moo", 23, "goo").pack(),
					t.get(space.get(Tuple.from(0x03, "kap@alp", 16911504L)).pack()).join());
			assertArrayEquals(Tuple.from(11, "zoo", 12, "ups", 13, "pop").pack(),
					t.get(space.get(Tuple.from(0x03, "bar@gor", 17829914L)).pack()).join());
			return null;
		});
	}
	
	@Test
	public void testListCategories() throws Exception {
		db.run((t) -> {
			t.set(space.get(Tuple.from(0x01, "xavier")).pack(),	TB);
			t.set(space.get(Tuple.from(0x01, "omega")).pack(),	TB);
			t.set(space.get(Tuple.from(0x01, "gamma")).pack(),	TB);
			t.set(space.get(Tuple.from(0x01, "epsilon")).pack(),TB);
			t.set(space.get(Tuple.from(0x01, "delta")).pack(),	TB);
			t.set(space.get(Tuple.from(0x01, "beta")).pack(),	TB);
			t.set(space.get(Tuple.from(0x01, "alpha")).pack(),	TB);
			return null;
		});
		
		List<String> actual = toList(service.listCategories());
		
		List<String> expected = Arrays.asList("alpha", "beta", "delta", "epsilon", "gamma", "omega", "xavier");
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListSymbols_All() throws Exception {
		db.run((t) -> {
			t.set(space.get(Tuple.from(0x02, "", "buggy")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "gamma", "gamma@boom")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "gamma", "gamma@best")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@hopper")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gattaca")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gummi")).pack(), TB);
			return null;
		});
		List<String> actual;
		
		actual = toList(service.listSymbols(new SymbolListRequest("", null, 500)));
		assertEquals(Arrays.asList("buggy"), actual);

		actual = toList(service.listSymbols(new SymbolListRequest("gamma", null, 500)));
		assertEquals(Arrays.asList("gamma@best", "gamma@boom"), actual);
		
		actual = toList(service.listSymbols(new SymbolListRequest("delta", null, 500)));
		assertEquals(Arrays.asList("delta@gattaca", "delta@gummi", "delta@hopper"), actual);
	}
	
	@Test
	public void testListSymbols_AfterSymbolAndLimit() throws Exception {
		db.run((t) -> {
			t.set(space.get(Tuple.from(0x02, "delta", "delta@buggy")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@boom")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@best")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@hopper")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gattaca")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gummi")).pack(), TB);
			return null;
		});
		List<String> actual;

		actual = toList(service.listSymbols(new SymbolListRequest("delta", "delta@boom", 3)));
		
		assertEquals(Arrays.asList("delta@buggy", "delta@gattaca", "delta@gummi"), actual);
	}
	
	@Test
	public void testListSymbols_AfterSymbolIsNotExistsAndBetweenOthers() throws Exception {
		db.run((t) -> {
			t.set(space.get(Tuple.from(0x02, "delta", "delta@buggy")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@boom")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@best")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@hopper")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gattaca")).pack(), TB);
			t.set(space.get(Tuple.from(0x02, "delta", "delta@gummi")).pack(), TB);
			return null;
		});
		List<String> actual;

		actual = toList(service.listSymbols(new SymbolListRequest("delta", "delta@boss", 3)));
		
		assertEquals(Arrays.asList("delta@buggy", "delta@gattaca", "delta@gummi"), actual);
	}
	
	@Test
	public void testListSymbols_ShouldUseDefaultLimitIsNotSpecified() throws Exception {
		int max_limit = service.getListSymbolsMaxLimit(), total_count = max_limit + 5;
		db.run((t) -> {
			for ( int i = 0; i < total_count; i ++ ) {
				t.set(space.get(Tuple.from(0x02, "delta", "delta@" + String.format("%03d", i))).pack(), TB);
			}
			return null;
		});
		
		List<String> actual = toList(service.listSymbols(new SymbolListRequest("delta", null, null)));
			
		List<String> expected = new ArrayList<>();
		for ( int i = 0; i < max_limit; i ++ ) {
			expected.add("delta@" + String.format("%03d", i));
		}
		Collections.sort(expected);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListSymbolUpdates() throws Exception {
		db.run((t) -> {
			t.set(space.get(Tuple.from(0x03, "foo@bar", 14281092L)).pack(),
				Tuple.from(2, "halo", 15, "24.096", 19, "zorro").pack());
			t.set(space.get(Tuple.from(0x03, "kap@mov", 15882689L)).pack(),
				Tuple.from(10, "foo", 11, "foo2", 12, "foo3").pack());
			t.set(space.get(Tuple.from(0x03, "kap@mov", 15900881L)).pack(),
				Tuple.from(12, "480", 13, "farenheit", 14, "billy").pack());
			t.set(space.get(Tuple.from(0x03, "kap@mov", 16099918L)).pack(),
				Tuple.from(29, "xxx", 30, "yyy", 31, "zzz").pack());
			t.set(space.get(Tuple.from(0x03, "zet@foo", 17088829L)).pack(),
				Tuple.from(49, "26.14", 990, "0.0001", 1000, "limited").pack());
			t.set(space.get(Tuple.from(0x03, "zet@foo", 19085471L)).pack(),
				Tuple.from(49, "25.95", 990, "0.0002", 1000, "unlimited").pack());
			return null;
		});
		List<SymbolUpdate> expected, actual;
		
		actual = toList(service.listSymbolUpdates("foo@bar"));
		expected = Arrays.asList(
				new SymbolUpdate("foo@bar", 14281092L, toMap(2, "halo", 15, "24.096", 19, "zorro"))
			);
		assertEquals(expected, actual);
		
		actual = toList(service.listSymbolUpdates("kap@mov"));
		expected = Arrays.asList(
				new SymbolUpdate("kap@mov", 15882689L, toMap(10, "foo", 11, "foo2", 12, "foo3")),
				new SymbolUpdate("kap@mov", 15900881L, toMap(12, "480", 13, "farenheit", 14, "billy")),
				new SymbolUpdate("kap@mov", 16099918L, toMap(29, "xxx", 30, "yyy", 31, "zzz"))
			);
		assertEquals(expected, actual);
		
		actual = toList(service.listSymbolUpdates("zet@foo"));
		expected = Arrays.asList(
				new SymbolUpdate("zet@foo", 17088829L, toMap(49, "26.14", 990, "0.0001", 1000, "limited")),
				new SymbolUpdate("zet@foo", 19085471L, toMap(49, "25.95", 990, "0.0002", 1000, "unlimited"))
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testClear() throws Exception {
		service.registerSymbol("tukana@batari");
		service.registerSymbol("kappa");
		service.registerSymbolUpdate(new SymbolUpdate("kap@mov", 15882689L, toMap(10, "foo", 11, "bar", 12, "buz")));
		service.registerSymbolUpdate(new SymbolUpdate("kap@alp", 16788992L, toMap(20, "alp", 30, "zip", 40, "gap")));
		service.registerSymbolUpdate(new SymbolUpdate("kap@alp", 16911504L, toMap(21, "boo", 22, "moo", 23, "goo")));
		service.registerSymbolUpdate(new SymbolUpdate("bar@gor", 17829914L, toMap(11, "zoo", 12, "ups", 13, "pop")));
		
		service.clear();
		
		assertEquals(Arrays.asList(), toList(service.listCategories()));
		assertEquals(Arrays.asList(), toList(service.listSymbols(new SymbolListRequest("kap", null, 1000))));
		assertEquals(Arrays.asList(), toList(service.listSymbols(new SymbolListRequest("bar", null, 1000))));
		assertEquals(Arrays.asList(), toList(service.listSymbolUpdates("kap@mov")));
		assertEquals(Arrays.asList(), toList(service.listSymbolUpdates("kap@alp")));
		assertEquals(Arrays.asList(), toList(service.listSymbolUpdates("bar@gor")));
	}

}
