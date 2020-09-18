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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.EventsBuilder;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.EventListRequest;
import ru.prolib.caelum.symboldb.SymbolListRequest;

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
				new FDBSchema(helper.getTestSubspace()), 100, 5);
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
	public void testRegisterEvents() {
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@mov")
				.withTime(15882689L)
				.withEvent(10, "foo")
				.withEvent(11, "bar")
				.withEvent(12, "buz")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16788992L)
				.withEvent(20, "alp")
				.withEvent(30, "zip")
				.withEvent(40, "gap")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16911504L)
				.withEvent(21, "boo")
				.withEvent(22, "moo")
				.withEvent(23, "goo")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("bar@gor")
				.withTime(17829914L)
				.withEvent(11, "zoo")
				.withEvent(12, "ups")
				.withEvent(13, "pop")
				.build());
		
		db.run((t) -> {
			// categories
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "kap")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x01, "bar")).pack()).join());
			// symbols
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "kap", "kap@mov")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "kap", "kap@alp")).pack()).join());
			assertArrayEquals(TB, t.get(space.get(Tuple.from(0x02, "bar", "bar@gor")).pack()).join());
			// events
			Tuple
			x = Tuple.from(0x03, "kap@mov", 15882689L);
			assertArrayEquals("foo".getBytes(), t.get(space.get(x.add(10)).pack()).join()); 
			assertArrayEquals("bar".getBytes(), t.get(space.get(x.add(11)).pack()).join());
			assertArrayEquals("buz".getBytes(), t.get(space.get(x.add(12)).pack()).join());
			
			x = Tuple.from(0x03, "kap@alp", 16788992L);
			assertArrayEquals("alp".getBytes(), t.get(space.get(x.add(20)).pack()).join());
			assertArrayEquals("zip".getBytes(), t.get(space.get(x.add(30)).pack()).join());
			assertArrayEquals("gap".getBytes(), t.get(space.get(x.add(40)).pack()).join());

			x = Tuple.from(0x03, "kap@alp", 16911504L);
			assertArrayEquals("boo".getBytes(), t.get(space.get(x.add(21)).pack()).join());
			assertArrayEquals("moo".getBytes(), t.get(space.get(x.add(22)).pack()).join());
			assertArrayEquals("goo".getBytes(), t.get(space.get(x.add(23)).pack()).join());
			
			x = Tuple.from(0x03, "bar@gor", 17829914L);
			assertArrayEquals("zoo".getBytes(), t.get(space.get(x.add(11)).pack()).join());
			assertArrayEquals("ups".getBytes(), t.get(space.get(x.add(12)).pack()).join());
			assertArrayEquals("pop".getBytes(), t.get(space.get(x.add(13)).pack()).join());
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
	public void testListCategories_ShouldBeOkIfNoCategories() throws Exception {
		
		List<String> actual = toList(service.listCategories());
		
		assertEquals(Arrays.asList(), actual);
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
	
	static void storeEvents(Transaction t, String symbol, long time, Object ...events) {
		if ( events.length % 2 != 0 ) {
			throw new IllegalArgumentException();
		}
		Tuple x = Tuple.from(0x03, symbol, time);
		for ( int i = 0; i < events.length / 2; i ++ ) {
			int event_id = (int) events[i * 2];
			String event_data = (String) events[i * 2 + 1];
			t.set(space.get(x.add(event_id)).pack(), event_data.getBytes());
		}
	}
	
	@Test
	public void testListEvents() throws Exception {
		db.run((t) -> {
			storeEvents(t, "foo@bar", 14281092L, 2, "halo", 15, "24.096", 19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1", 11, "foo2", 12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480", 13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx", 30, "yyy", 31, "zzz");
			storeEvents(t, "zet@foo", 17088829L, 49, "26.14", 990, "0.0001", 1000, "limited");
			storeEvents(t, "zet@foo", 19085471L, 49, "25.95", 990, "0.0002", 1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("foo@bar")));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("foo@bar")
					.withTime(14281092L)
					.withEvent(2, "halo")
					.withEvent(15, "24.096")
					.withEvent(19, "zorro")
					.build()
			);
		assertEquals(expected, actual);
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov")));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15882689L)
					.withEvent(10, "foo1")
					.withEvent(11, "foo2")
					.withEvent(12, "foo3")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15900881L)
					.withEvent(12, "480")
					.withEvent(13, "farenheit")
					.withEvent(14, "billy")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(16099918L)
					.withEvent(29, "xxx")
					.withEvent(30, "yyy")
					.withEvent(31, "zzz")
					.build()
			);
		assertEquals(expected, actual);
		
		actual = toList(service.listEvents(new EventListRequest("zet@foo")));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("zet@foo")
					.withTime(17088829L)
					.withEvent(49, "26.14")
					.withEvent(990, "0.0001")
					.withEvent(1000, "limited")
					.build(),
				new EventsBuilder()
					.withSymbol("zet@foo")
					.withTime(19085471L)
					.withEvent(49, "25.95")
					.withEvent(990, "0.0002")
					.withEvent(1000, "unlimited")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_ShouldUseFromIfDefined() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", 16000000L, null, null)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(16099918L)
					.withEvent(29, "xxx")
					.withEvent(30, "yyy")
					.withEvent(31, "zzz")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(17088829L)
					.withEvent(49, "26.14")
					.withEvent(990, "0.0001")
					.withEvent(1000, "limited")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(19085471L)
					.withEvent(49, "25.95")
					.withEvent(990, "0.0002")
					.withEvent(1000, "unlimited")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_TimeFromShouldBeInclusive() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", 17088829L, null, null)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(17088829L)
					.withEvent(  49, "26.14")
					.withEvent( 990, "0.0001")
					.withEvent(1000, "limited")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(19085471L)
					.withEvent(  49, "25.95")
					.withEvent( 990, "0.0002")
					.withEvent(1000, "unlimited")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_ShouldUseToIfDefined() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", null, 16000000L, null)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(14281092L)
					.withEvent( 2, "halo")
					.withEvent(15, "24.096")
					.withEvent(19, "zorro")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15882689L)
					.withEvent(10, "foo1")
					.withEvent(11, "foo2")
					.withEvent(12, "foo3")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15900881L)
					.withEvent(12, "480")
					.withEvent(13, "farenheit")
					.withEvent(14, "billy")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_TimeToShouldBeExclusive() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", null, 15900881L, null)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(14281092L)
					.withEvent( 2, "halo")
					.withEvent(15, "24.096")
					.withEvent(19, "zorro")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15882689L)
					.withEvent(10, "foo1")
					.withEvent(11, "foo2")
					.withEvent(12, "foo3")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_ShouldUseRequestedLimitIfLessThanMax() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", 15000000L, null, 2)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15882689L)
					.withEvent(10, "foo1")
					.withEvent(11, "foo2")
					.withEvent(12, "foo3")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15900881L)
					.withEvent(12, "480")
					.withEvent(13, "farenheit")
					.withEvent(14, "billy")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testListEvents_ShouldUseMaxLimitIfRequestedLimitIsGreater() throws Exception {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});
		List<Events> expected, actual;
		
		actual = toList(service.listEvents(new EventListRequest("kap@mov", null, null, 7)));
		expected = Arrays.asList(
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(14281092L)
					.withEvent( 2, "halo")
					.withEvent(15, "24.096")
					.withEvent(19, "zorro")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15882689L)
					.withEvent(10, "foo1")
					.withEvent(11, "foo2")
					.withEvent(12, "foo3")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(15900881L)
					.withEvent(12, "480")
					.withEvent(13, "farenheit")
					.withEvent(14, "billy")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(16099918L)
					.withEvent(29, "xxx")
					.withEvent(30, "yyy")
					.withEvent(31, "zzz")
					.build(),
				new EventsBuilder()
					.withSymbol("kap@mov")
					.withTime(17088829L)
					.withEvent(  49, "26.14")
					.withEvent( 990, "0.0001")
					.withEvent(1000, "limited")
					.build()
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testDeleteEvents() {
		db.run((t) -> {
			storeEvents(t, "kap@mov", 14281092L,  2, "halo",   15, "24.096",    19, "zorro");
			storeEvents(t, "kap@mov", 15882689L, 10, "foo1",   11, "foo2",      12, "foo3");
			storeEvents(t, "kap@mov", 15900881L, 12, "480",    13, "farenheit", 14, "billy");
			storeEvents(t, "kap@mov", 16099918L, 29, "xxx",    30, "yyy",       31, "zzz");
			storeEvents(t, "kap@mov", 17088829L, 49, "26.14", 990, "0.0001",  1000, "limited");
			storeEvents(t, "kap@mov", 19085471L, 49, "25.95", 990, "0.0002",  1000, "unlimited");
			return null;
		});

		service.deleteEvents(Arrays.asList(
				new Events("kappa@mov", 15882689L, toMap( 11, "delete", 12, "delete")),
				new Events("kappa@mov", 17088829L, toMap(990, "delete", 49, "delete")),
				new Events("kappa@mov", 14281092L, toMap( 15, "delete"))
			));
		
		db.run((t) -> {
			assertNull(t.get(space.get(Tuple.from(0x03, "kappa@mov", 14281092L,  15)).pack()).join());
			assertNull(t.get(space.get(Tuple.from(0x03, "kappa@mov", 15882689L,  11)).pack()).join());
			assertNull(t.get(space.get(Tuple.from(0x03, "kappa@mov", 15882689L,  12)).pack()).join());
			assertNull(t.get(space.get(Tuple.from(0x03, "kappa@mov", 17088829L, 990)).pack()).join());
			assertNull(t.get(space.get(Tuple.from(0x03, "kappa@mov", 17088829L,  49)).pack()).join());
			return null;
		});
	}
		
	@Test
	public void testClear_ShouldClearIfGlobal() throws Exception {
		service.registerSymbol("tukana@batari");
		service.registerSymbol("kappa");
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@mov")
				.withTime(15882689L)
				.withEvent(10, "foo")
				.withEvent(11, "bar")
				.withEvent(12, "buz")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16788992L)
				.withEvent(20, "alp")
				.withEvent(30, "zip")
				.withEvent(40, "gap")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16911504L)
				.withEvent(21, "boo")
				.withEvent(22, "moo")
				.withEvent(23, "goo")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("bar@gor")
				.withTime(17829914L)
				.withEvent(11, "zoo")
				.withEvent(12, "ups")
				.withEvent(13, "pop")
				.build());
		
		service.clear(true);
		
		assertEquals(Arrays.asList(), toList(service.listCategories()));
		assertEquals(Arrays.asList(), toList(service.listSymbols(new SymbolListRequest("kap", null, 1000))));
		assertEquals(Arrays.asList(), toList(service.listSymbols(new SymbolListRequest("bar", null, 1000))));
		assertEquals(Arrays.asList(), toList(service.listEvents(new EventListRequest("kap@mov"))));
		assertEquals(Arrays.asList(), toList(service.listEvents(new EventListRequest("kap@alp"))));
		assertEquals(Arrays.asList(), toList(service.listEvents(new EventListRequest("bar@gor"))));
	}
	
	@Test
	public void testClear_ShouldSkipIfLocal() throws Exception {
		service.registerSymbol("tukana@batari");
		service.registerSymbol("kappa");
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@mov")
				.withTime(15882689L)
				.withEvent(10, "foo")
				.withEvent(11, "bar")
				.withEvent(12, "buz")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16788992L)
				.withEvent(20, "alp")
				.withEvent(30, "zip")
				.withEvent(40, "gap")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("kap@alp")
				.withTime(16911504L)
				.withEvent(21, "boo")
				.withEvent(22, "moo")
				.withEvent(23, "goo")
				.build());
		service.registerEvents(new EventsBuilder()
				.withSymbol("bar@gor")
				.withTime(17829914L)
				.withEvent(11, "zoo")
				.withEvent(12, "ups")
				.withEvent(13, "pop")
				.build());
		
		service.clear(false);

		assertEquals(Arrays.asList("", "bar", "kap", "tukana"), toList(service.listCategories()));
		assertEquals(Arrays.asList("bar@gor"), toList(service.listSymbols(new SymbolListRequest("bar", null, 1000))));
		assertEquals(Arrays.asList("kap@alp", "kap@mov"),
				toList(service.listSymbols(new SymbolListRequest("kap", null, 1000))));
	}

}
