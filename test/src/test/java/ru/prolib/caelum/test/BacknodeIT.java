package ru.prolib.caelum.test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.awaitility.Awaitility.await;
import static ru.prolib.caelum.test.ApiTestHelper.*;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import io.restassured.specification.RequestSpecification;
import ru.prolib.caelum.test.dto.CategoriesResponseDTO;
import ru.prolib.caelum.test.dto.ItemResponseDTO;
import ru.prolib.caelum.test.dto.ItemsResponseDTO;
import ru.prolib.caelum.test.dto.PeriodsResponseDTO;
import ru.prolib.caelum.test.dto.PingResponseDTO;
import ru.prolib.caelum.test.dto.SymbolResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdateDTO;
import ru.prolib.caelum.test.dto.SymbolUpdateResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdatesResponseDTO;
import ru.prolib.caelum.test.dto.SymbolsResponseDTO;
import ru.prolib.caelum.test.dto.TuplesResponseDTO;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BacknodeIT {
	static final ApiTestHelper ath = new ApiTestHelper(false, false);
	
	@Rule(order=Integer.MIN_VALUE)
	public TestWatcher watchman = new TestWatcher() {
		
		private String toMarker(Description descr) {
			return descr.getClassName() + "#" + descr.getMethodName();
		}
		
		@Override
		protected void starting(Description descr) {
			ath.apiLogMarker("Starting " + toMarker(descr));
		}
		
		@Override
		protected void finished(Description descr) {
			ath.apiLogMarker("Finished " + toMarker(descr));
		}
		
	};
	
	@BeforeClass
	public static void setUpBeforeClass() {
		ath.setBacknodeHosts(Arrays.asList("localhost:9698"));
		ath.setUpBeforeClass();
	}
	
	@Before
	public void setUp() {
		ath.setUp();
	}
	
	@After
	public void tearDown() {
		ath.tearDown();
	}
	
	@AfterClass
	public static void tearDownAfterClass() {
		ath.tearDownAfterClass();
	}
	
	@Test
	public void C0000_Ping() {
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			PingResponseDTO response = ath.apiPing(spec);
			assertNotError(response);
			assertNull(response.data);
		}
	}
	
	// C1*** - Test Cases of features related to items
	
	@Test
	public void C1001_PutItem_PutAndTestResponse() {
		CatSym cs = ath.newSymbol();
		Item item1 = ath.registerItem(cs, ath.getRecentItemTimePlus1());

		ItemResponseDTO response = ath.apiPutItem(ath.getSpecRandom(), item1);
		
		assertNotError(response);
	}
	
	@Test
	public void C1002_PutItem_PutOfSameTimeAndTestSequence() throws Exception {
		long time = ath.getRecentItemTimePlus1();
		CatSym cs = ath.newSymbol();
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time)));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time)));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time)));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time)));
		
		CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
		waitUntil(() -> {
			ItemsResponseDTO response = ath.apiGetItems(cs.symbol);
			assertNotError(response);
			return response.data.rows.size() == 4 ? r.complete(response) : false;
		});
		ItemsResponseDTO response = r.get(1, TimeUnit.SECONDS);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("std", response.data.format);
		assertNotNull(response.data.magic);
		assertThat(response.data.magic.length(), is(greaterThanOrEqualTo(1)));
		assertEquals(4, response.data.rows.size());
		assertEquals(ath.registeredItems(cs), toItems(cs, response.data.rows));
	}
	
	@Test
	public void C1003_PutItem_ItemsOfDifferentSymbolsShouldBeOkTogether() {
		long time = ath.getRecentItemTimePlus1();
		List<CatSym> symbols = new ArrayList<>();
		for ( int i = 1; i < 7; i ++ ) {
			CatSym cs = ath.newSymbol();
			symbols.add(cs);
			for ( int j = 0; j < i; j ++ ) {
				assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time)));
				time ++;
			}
		}
		
		for ( int i = 0; i < symbols.size(); i ++ ) {
			CatSym cs = symbols.get(i);
			ItemsResponseDTO response = ath.apiGetItems(cs.symbol);
			assertEquals(i + 1, response.data.rows.size());
			assertEquals(ath.registeredItems(cs), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1004_PutItem_ShouldRegisterCategories() throws Exception {
		long time = ath.getRecentItemTimePlus1();
		List<CatSym> cs_list = ath.newSymbols(5, 10);
		for ( CatSym cs : cs_list ) {
			ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time));
		}
		
		List<String> expected = ath.registeredCategories();
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<CategoriesResponseDTO> r = new CompletableFuture<>();
			waitUntil(() -> {
				CategoriesResponseDTO response = ath.apiGetCategories(spec);
				assertNotError(response);
				return response.data.rows.size() == expected.size() ? r.complete(response) : false;
			});
			CategoriesResponseDTO response = r.get(1, TimeUnit.SECONDS);
			assertEquals(expected, response.data.rows);
		}
	}
	
	@Test
	public void C1005_PutItem_ShouldRegisterSymbols() throws Exception {
		long time = ath.getRecentItemTimePlus1();
		for ( CatSym cs : ath.newSymbols(5, 10) ) {
			ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time ++));
		}
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			for ( String category : ath.registeredCategories() ) {
				final List<String> expected_symbols = ath.registeredSymbols(category);
				CompletableFuture<SymbolsResponseDTO> r = new CompletableFuture<>();
				waitUntil(() -> {
						SymbolsResponseDTO response = ath.apiGetSymbols(spec, category);
						assertNotError(response);
						return response.data.rows.size() == expected_symbols.size() ? r.complete(response) : false;
					});
				SymbolsResponseDTO response = r.get(1, TimeUnit.SECONDS);
				
				assertNotError(response);
				assertEquals(ath.registeredSymbols(category), response.data.rows);
			}
		}
	}
	
	@Test
	public void C1006_PutItem_BatchModeShouldWorkOk() {
		long time = ath.getRecentItemTimePlus1();
		CatSym cs1 = ath.newSymbol(), cs2 = ath.newSymbol();
		List<Item> items = new ArrayList<>();
		for ( int i = 0; i < 50; i ++ ) {
			items.add(ath.registerItem(cs1, time));
			items.add(ath.registerItem(cs2, time ++));
		}
		
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), items));
		
		ItemsResponseDTO response;
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			assertNotError(response = ath.apiGetItems(spec, cs1.symbol, null));
			assertEquals(50, response.data.rows.size());
			assertEquals(ath.registeredItems(cs1), toItems(cs1, response.data.rows));
			
			assertNotError(response = ath.apiGetItems(spec, cs2.symbol, null));
			assertEquals(50, response.data.rows.size());
			assertEquals(ath.registeredItems(cs2), toItems(cs2, response.data.rows));
		}
	}
	
	@Test
	public void C1007_PutItem_PutEqualItemsShouldBeOk() throws Exception {
		long time = ath.getRecentItemTimePlus1();
		CatSym cs = ath.newSymbol();
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time, "1.250", "1000")));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time, "1.250", "1000")));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time, "1.250", "1000")));
		assertNotError(ath.apiPutItem(ath.getSpecRandom(), ath.registerItem(cs, time, "1.250", "1000")));
		
		CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
		waitUntil(() -> {
			ItemsResponseDTO response = ath.apiGetItems(cs.symbol);
			assertNotError(response);
			return response.data.rows.size() == 4 ? r.complete(response) : false;
		});
		ItemsResponseDTO response = r.get(1, TimeUnit.SECONDS);
		for ( Item item : toItems(cs, response.data.rows) ) {
			assertEquals(cs.newItem(time, "1.250", "1000"), item);
		}
	}

	@Test
	public void C1050_Items_AllShouldBeLimitedUpTo5000() throws Exception {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 30000L; // +30 seconds for each
		int total_m1_tuples = 4 * 24 * 60, total_items = (int) (total_m1_tuples * 60000 / time_delta);
		assertThat(total_items, is(greaterThan(5000 * 2)));
		ath.generateItems(cs, total_items, start_time, time_delta, "0.001", "0.001", "5", "5");

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null);
			assertNotError(response);
			assertEquals(5000, response.data.rows.size());
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);	
			assertThat(response.data.fromOffset, is(greaterThanOrEqualTo(5000L)));
			assertNotNull(response.data.magic);
			assertThat(response.data.magic.length(), is(equalTo(32)));
			assertEquals(ath.registeredItems(cs).subList(0,  5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1051_Items_WithLimitLessThanMaxLimitAndLessThanItemsCount() {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 800, ath.getRecentItemTimePlus1(), 15000L, "0.050", "0.025", "1", "1");
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 500);
			
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(500, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(ath.registeredItems(cs).subList(0, 500), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1052_Items_WithLimitLessThanMaxLimitButGreaterThanItemsCount() {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 800, ath.getRecentItemTimePlus1(), 15000L, "0.050", "0.025", "1", "2");
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 1000);
			
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(800, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(ath.registeredItems(cs).subList(0, 800), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1053_Items_WithLimitGreaterThanMaxLimitAndLessThanItemsCount() {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 5500, ath.getRecentItemTimePlus1(), 15000L, "1025.0", "0.5", "1000", "10");
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 5200);
			
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(5000, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(5500, ath.registeredItems(cs).size());
			assertEquals(ath.registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1054_Items_WithLimitGreaterThanMaxLimitAndGreaterThanItemsCount() {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 5500, ath.getRecentItemTimePlus1(), 15000L, "100.24919", "0.00001", "10900", "-1");
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 7000);
			
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(5000, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(5500, ath.registeredItems(cs).size());
			assertEquals(ath.registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1055_Items_ShouldConsiderTimeFromInclusiveAndTimeToExclusive() throws Exception {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 1000, start_time + 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = ath.registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem(start_time +  200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem(start_time +  300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(start_time + 1099000L, "0.15505",  "900"), expected.get(899));
		assertEquals(                 cs.newItem(start_time + 1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 900);

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null,
						start_time + 300000L, start_time + 1100000L);
				assertNotError(response);
				return response.data.rows.size() == 800 ? r.complete(response) : false;
			});
			ItemsResponseDTO response = r.get(1, TimeUnit.SECONDS);
			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(800, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1056_Items_ShouldConsiderTimeFromInclusive() {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 1000, start_time + 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = ath.registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem(start_time +  200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem(start_time +  300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(start_time + 1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 1000);
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null, start_time + 300000L, null);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(900, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1057_Items_ShouldConsiderTimeToExclusive() throws Exception {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 1000, start_time + 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = ath.registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(expected_first = cs.newItem(start_time +  200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_last  = cs.newItem(start_time + 1099000L, "0.15505",  "900"), expected.get(899));
		assertEquals(                 cs.newItem(start_time + 1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(000, 900);

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null, null, start_time + 1100000L);
				assertNotError(response);
				return response.data.rows.size() == 900 ? r.complete(response) : false;
			});
			ItemsResponseDTO response = r.get(1, TimeUnit.SECONDS);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEqualsItemByItem("Test failed", expected, actual);
		}
	}
	
	@Test
	public void C1058_Items_LimitShouldHaveGreaterPriorityThanTimeTo() {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 1000, start_time + 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = ath.registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem(start_time +  200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem(start_time +  300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(start_time + 1049000L, "0.15755",  "850"), expected.get(849));
		assertEquals(                 cs.newItem(start_time + 1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 850);

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			ItemsResponseDTO response =
					ath.apiGetItems(spec, cs.symbol, 750, start_time + 300000L, start_time + 1100000L);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(750, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1059_Items_ShouldConsiderFromOffset() throws Exception {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 12000, start_time + 230000L, 1000L, "1.0000", "0.0005", "1", "1");
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			final CompletableFuture<ItemsResponseDTO> r1 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null);
				assertNotError(response);
				return response.data.rows.size() == 5000 ? r1.complete(response) : false;
			});
			final ItemsResponseDTO response1 = r1.get(1, TimeUnit.SECONDS);
			assertEquals(ath.registeredItems(cs).subList(0, 5000), toItems(cs, response1.data.rows));
			
			final CompletableFuture<ItemsResponseDTO> r2 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null, null, null,
						response1.data.magic, response1.data.fromOffset);
				assertNotError(response);
				return response.data.rows.size() == 5000 ? r2.complete(response) : false;
			});
			final ItemsResponseDTO response2 = r2.get(1, TimeUnit.SECONDS); 
			assertEquals(cs.symbol, response2.data.symbol);
			assertEquals("std", response2.data.format);
			assertNotNull(response2.data.magic);
			assertThat(response2.data.magic.length(), is(greaterThan(0)));
			assertNotNull(response2.data.fromOffset);
			assertEquals(ath.registeredItems(cs).subList(5000, 10000), toItems(cs, response2.data.rows));
			
			final CompletableFuture<ItemsResponseDTO> r3 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null, null, null,
						response2.data.magic, response2.data.fromOffset);
				assertNotError(response);
				return response.data.rows.size() == 2000 ? r3.complete(response) : false;
			});
			final ItemsResponseDTO response3 = r3.get(1, TimeUnit.SECONDS); 
			assertEquals(cs.symbol, response3.data.symbol);
			assertEquals("std", response3.data.format);
			assertNotNull(response3.data.magic);
			assertThat(response3.data.magic.length(), is(greaterThan(0)));
			assertNotNull(response3.data.fromOffset);
			assertEquals(ath.registeredItems(cs).subList(10000, 12000), toItems(cs, response3.data.rows));
		}
	}
	
	@Test
	public void C1060_Items_ShouldIgnoreTimeFromIfFromOffsetSpecified() throws Exception {
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs, 6000, start_time + 230000L, 1000L, "1.0000", "0.0005", "1", "1");

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r1 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null);
				assertNotError(response);
				return response.data.rows.size() == 5000 ? r1.complete(response) : false;
			});
			ItemsResponseDTO response1 = r1.get(1, TimeUnit.SECONDS);
			assertEqualsItemByItem("#1", ath.registeredItems(cs).subList(0, 5000), toItems(cs, response1.data.rows));
			
			CompletableFuture<ItemsResponseDTO> r2 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null, start_time + 730000L, null,
					response1.data.magic, response1.data.fromOffset);
				assertNotError(response);
				return response.data.rows.size() == 1000 ? r2.complete(response) : false;
			});
			ItemsResponseDTO response2 = r2.get(1, TimeUnit.SECONDS); 
			assertEqualsItemByItem("#2", ath.registeredItems(cs).subList(5000, 6000), toItems(cs, response2.data.rows));
		}
	}
	
	@Test
	public void C1061_Items_ShouldBeOkIfOutOfRangeIfFromOffsetSpecified() throws Exception {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 6000, ath.getRecentItemTimePlus1(), 1000L, "1.0000", "0.0005", "1", "1");
		
		List<Item> expected_rows = ath.registeredItems(cs).subList(0, 5000);
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r1 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null);
				assertNotError(response);
				return response.data.rows.size() == 5000 ? r1.complete(response) : false;
			});
			ItemsResponseDTO response1 = r1.get(1, TimeUnit.SECONDS);
			assertEqualsItemByItem("Initial request", expected_rows, toItems(cs, response1.data.rows));
			
			CompletableFuture<ItemsResponseDTO> r2 = new CompletableFuture<>();
			waitUntil(() -> {
				ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 1, null, null,
						response1.data.magic, response1.data.fromOffset + 1128276L);
				assertNotError(response);
				return response.data.rows.size() == 0 ? r2.complete(response) : false;
			});
			ItemsResponseDTO response2 = r2.get(1, TimeUnit.SECONDS);
			assertEquals(0, response2.data.rows.size());
		}
	}	
	
	@Test
	public void C1062_Items_ConcurrentRequestsShouldGiveSameResults() throws Exception {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 6000);
		int num_threads = 5;
		CountDownLatch started = new CountDownLatch(num_threads),
			go = new CountDownLatch(1), finished = new CountDownLatch(num_threads);
		RequestSpecification spec = ath.getSpecRandom();
		List<CompletableFuture<ItemsResponseDTO>> result = new ArrayList<>();
		for ( int i = 0; i < num_threads; i ++ ) {
			final CompletableFuture<ItemsResponseDTO> f = new CompletableFuture<>();
			new Thread() {
				@Override
				public void run() {
					try {
						started.countDown();
						if ( go.await(5, TimeUnit.SECONDS) ) {
							ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, null);
							assertNotError(response);
							f.complete(response);
							finished.countDown();
						}
					} catch ( Exception e ) {
						e.printStackTrace();
					}
				}
			}.start();
			result.add(f);
		}
		
		go.countDown();
		
		CompletableFuture.allOf(result.toArray(new CompletableFuture<?>[0])).get(5, TimeUnit.SECONDS);
		List<Item> expected = ath.registeredItems(cs).subList(0, 5000);
		for ( int i = 0; i < num_threads; i ++ ) {
			CompletableFuture<ItemsResponseDTO> f = result.get(i);
			assertEqualsItemByItem("Thread #" + i, expected, toItems(cs, f.get().data.rows));
		}
	}

	@Test
	public void C1063_Items_TwoConsecutiveCallsShouldGiveSameResult() throws Exception {
		CatSym cs = ath.newSymbol();
		ath.generateItems(cs, 4000);

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
			await().dontCatchUncaughtExceptions()
				.pollInterval(Duration.ofSeconds(5L))
				.atMost(Duration.ofSeconds(20))
				.until(() -> {
					ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 3000);
					assertNotError(response);
					if ( response.data.rows.size() == 3000 ) {
						r.complete(response);
						return true;
					} else {
						return false;
					}
				});
			
			ItemsResponseDTO response1, response2;
			assertNotError(response1 = r.get(1, TimeUnit.SECONDS));
			assertNotError(response2 = ath.apiGetItems(spec, cs.symbol, 3000));
			
			assertEquals(3000, response1.data.rows.size());
			assertEquals(response1.data.symbol, response2.data.symbol);
			assertEquals(response1.data.format, response2.data.format);
			assertEquals(response1.data.rows, response2.data.rows);
			assertEquals(response1.data.magic, response2.data.magic);
			assertEquals(response1.data.fromOffset, response2.data.fromOffset);
		}
	}
	
	@Test
	public void C1064_Items_AnyNodeShouldProvideDataOfAnySymbolDespitePartitioning() {
		Map<Integer, List<CatSym>> map = ath.newSymbolsOfDifferentPartitions(5);
		List<CatSym> cs_list = map.values().stream()
			.flatMap(x -> x.stream())
			.collect(Collectors.toList());
		ath.generateItems(cs_list, 1000);
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			for ( int num_partitions : map.keySet() ) {
				for ( CatSym cs : map.get(num_partitions) ) {
					ItemsResponseDTO response = ath.apiGetItems(spec, cs.symbol, 877);
					assertNotError(response);
					assertEquals(ath.registeredItems(cs).subList(0,  877), toItems(cs, response.data.rows));
				}
			}
		}
	}
	
	// C2*** - Test Cases of features related to symbol and symbol updates
	
	@Test
	public void C2010_PutSymbol_PutAndTestResponse() {
		CatSym cs = ath.newSymbol();

		SymbolResponseDTO response = ath.apiPutSymbol(ath.getSpecRandom(), cs.symbol);
		
		assertNotError(response);
	}
	
	@Test
	public void C2011_PutSymbol_ShouldBeOkIfEmptyCategory() {
		CatSym cs = ath.newSymbol("");
		
		assertNotError(ath.apiPutSymbol(ath.getSpecRandom(), cs.symbol));
		
		CategoriesResponseDTO response = ath.apiGetCategories();
		assertNotError(response);
		assertTrue(response.data.rows.contains(""));
	}
	
	@Test
	public void C2012_PutSymbol_ShouldRegisterCategory() {
		List<CatSym> symbols = ath.newSymbols(2, 1);
		CatSym cs1 = symbols.get(0), cs2 = symbols.get(1);
		
		assertNotError(ath.apiPutSymbol(ath.getSpecRandom(), cs1.symbol));
		assertNotError(ath.apiPutSymbol(ath.getSpecRandom(), cs2.symbol));
		
		CategoriesResponseDTO response = ath.apiGetCategories();
		assertNotError(response);
		assertTrue(response.data.rows.contains(cs1.category));
		assertTrue(response.data.rows.contains(cs2.category));
	}
	
	@Test
	public void C2012_PutSymbol_ShouldSupportBatchMode() {
		List<CatSym> symbols = ath.newSymbols(1, 20);
		
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		SymbolsResponseDTO response = ath.apiGetSymbols(ath.getSpecRandom(), category);
		assertNotError(response);
		assertEquals(20, response.data.rows.size());
		assertEquals(ath.registeredSymbols(category), response.data.rows);
	}
	
	@Test
	public void C2030_Symbols_GetAll() throws Exception {
		List<CatSym> symbols = ath.newSymbols(10, 20);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		Set<String> categories = symbols.stream().map(x -> x.category).collect(Collectors.toSet());
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			for ( String category : categories ) {
				CompletableFuture<SymbolsResponseDTO> r = new CompletableFuture<>();
				List<String> expected_symbols = ath.registeredSymbols(category);
				waitUntil(() -> {
					SymbolsResponseDTO response = ath.apiGetSymbols(spec, category);
					assertNotError(response);
					return response.data.rows.size() == expected_symbols.size() ? r.complete(response) : false;
				});
				SymbolsResponseDTO response = r.get(1, TimeUnit.SECONDS);
				assertEquals(expected_symbols, response.data.rows);
				assertThat(response.data.rows.size(), is(greaterThanOrEqualTo(20)));
			}
		}
	}
	
	@Test
	public void C2031_Symbols_ShouldApplyDefaultLimit() {
		List<CatSym> symbols = ath.newSymbols(1, 7000);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolsResponseDTO response = ath.apiGetSymbols(spec, category);
			assertNotError(response);
			assertEquals(ath.registeredSymbols(category).subList(0, 5000), response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}
	
	@Test
	public void C2032_Symbols_WithLimitGreaterThanMaxLimit() {
		List<CatSym> symbols = ath.newSymbols(1, 7000);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolsResponseDTO response = ath.apiGetSymbols(spec, category, 5100);
			assertNotError(response);
			assertEquals(ath.registeredSymbols(category).subList(0, 5000), response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}
	
	@Test
	public void C2033_Symbols_WithLimitGreaterThanMaxLimitAndAfterSymbol() {
		List<CatSym> symbols = ath.newSymbols(1, 7000);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		List<String> expected = ath.registeredSymbols(category);
		String afterSymbol = expected.get(199);
		expected = expected.subList(200, 5200);
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolsResponseDTO response = ath.apiGetSymbols(spec, category, afterSymbol, 8000);
			assertNotError(response);
			assertEquals(expected, response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}

	@Test
	public void C2034_Symbols_WithLimit() {
		List<CatSym> symbols = ath.newSymbols(5, 20);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));
		
		List<String> expected;
		SymbolsResponseDTO response;
		
		String expected_category = symbols.get(0).category;
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			response = ath.apiGetSymbols(spec, expected_category, 5);
			assertNotError(response);
			
			expected = ath.registeredSymbols(expected_category).subList(0,  5);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(5, response.data.rows.size());
		}
	}
	
	@Test
	public void C2035_Symbols_WithAfterSymbol() {
		List<CatSym> symbols = ath.newSymbols(5, 20);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));

		List<String> expected;
		SymbolsResponseDTO response;
		
		String expected_category = symbols.get(0).category;
		String after_symbol = ath.registeredSymbols(expected_category).get(2);
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			response = ath.apiGetSymbols(spec, expected_category, after_symbol); // 3-19
			assertNotError(response);
			
			expected = ath.registeredSymbols(expected_category).subList(3, 20);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(17, response.data.rows.size());
		}
	}
	
	@Test
	public void C2036_Symbols_WithLimitAndAfterSymbol() {
		List<CatSym> symbols = ath.newSymbols(5, 20);
		assertNotError(ath.apiPutSymbolCS(ath.getSpecRandom(), symbols));

		List<String> expected;
		SymbolsResponseDTO response;
		String expected_category = symbols.get(0).category;
		String after_symbol = ath.registeredSymbols(expected_category).get(2);
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			response = ath.apiGetSymbols(spec, expected_category, after_symbol, 15);
			assertNotError(response);
			
			expected = ath.registeredSymbols(expected_category).subList(3, 18);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(15, response.data.rows.size());
		}
	}
	
	@Test
	public void C2050_PutSymbolUpdate_PutAndTestResponse() {
		CatSym cs = ath.newSymbol();
		
		SymbolUpdateResponseDTO response =
				ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 112456L, 1, "foo", 2, "bar", 5, "buzz");
		
		assertNotError(response);
	}
	
	@Test
	public void C2051_PutSymbolUpdate_ShouldRegisterUpdate() {
		CatSym cs = ath.newSymbol();
		
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 237991L, 50, "pop", 51, "gap", 52, "die"));

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolUpdatesResponseDTO response = ath.apiGetSymbolUpdates(spec, cs.symbol);
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			List<SymbolUpdateDTO> expected = Arrays.asList(
					new SymbolUpdateDTO(237991L, toMap(50, "pop", 51, "gap", 52, "die"))
				);
			assertEquals(expected, response.data.rows);
		}
	}
	
	@Test
	public void C2052_PutSymbolUpdate_ShouldRegisterCategory() {
		CatSym cs = ath.newSymbol();
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279390L, 1, "foo"));
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CategoriesResponseDTO response = ath.apiGetCategories(spec);
			assertNotError(response);
			assertTrue(response.data.rows.contains(cs.category));
		}
	}
	
	@Test
	public void C2053_PutSymbolUpdate_ShouldRegisterSymbol() {
		CatSym cs = ath.newSymbol();
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279390L, 1, "foo"));

		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolsResponseDTO response = ath.apiGetSymbols(spec, cs.category);
			assertNotError(response);
			assertEquals(Arrays.asList(cs.symbol), response.data.rows);
		}
	}

	@Test
	public void C2054_PutSymbolUpdate_ShouldOverrideExistingUpdate() {
		CatSym cs = ath.newSymbol();
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279390L, 1, "foo"));
		
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279390L, 5, "back", 7, "rogers"));
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolUpdatesResponseDTO response = ath.apiGetSymbolUpdates(spec, cs.symbol);
			assertNotError(response);
			List<SymbolUpdateDTO> expected = Arrays.asList(
					new SymbolUpdateDTO(279390L, toMap(5, "back", 7, "rogers"))
				);
			assertEquals(expected, response.data.rows);
		}
	}
	
	@Test
	public void C2060_SymbolUpdates() {
		CatSym cs = ath.newSymbol();
		// the order does not matter
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279200L, 5, "back", 7, "rogers"));
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279100L, 1, "foo"));
		assertNotError(ath.apiPutSymbolUpdate(ath.getSpecRandom(), cs.symbol, 279000L, 3, "mamba", 4, "garpia"));
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolUpdatesResponseDTO response = ath.apiGetSymbolUpdates(spec, cs.symbol);
			assertNotError(response);
			List<SymbolUpdateDTO> expected = Arrays.asList(
					new SymbolUpdateDTO(279000L, toMap(3, "mamba", 4, "garpia")),
					new SymbolUpdateDTO(279100L, toMap(1, "foo")),
					new SymbolUpdateDTO(279200L, toMap(5, "back", 7, "rogers"))
				);
			assertEquals(expected, response.data.rows);
		}
	}
	
	// C3*** - Test Cases of features related to categories
	
	@Test
	public void C3001_Symbols_ShouldReturnSymbolsOfEmptyCategory() {
		assertNotError(ath.apiPutSymbol(ath.getSpecRandom(),
				Arrays.asList("kobresia", "canopus", "foo@sirius", "bar@io")));
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			SymbolsResponseDTO response = ath.apiGetSymbols(spec, "");
			assertNotError(response);
			assertTrue(response.data.rows.contains("canopus"));
			assertTrue(response.data.rows.contains("kobresia"));
		}
	}
	
	@Test
	public void C3002_Categories_ShouldReturnAllCategoriesIncludingEmpty() {
		assertNotError(ath.apiPutSymbol(ath.getSpecRandom(),
				Arrays.asList("kobresia", "canopus", "foo@sirius", "bar@io")));
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			CategoriesResponseDTO response = ath.apiGetCategories(spec);
			assertNotError(response);
			assertTrue(response.data.rows.contains(""));
			assertTrue(response.data.rows.contains("bar"));
			assertTrue(response.data.rows.contains("foo"));
		}
	}
	
	// C9*** - Test Cases of features related to tuples
	
	@Test
	public void C9001_Tuples_SimpleM1Request() throws Exception {
		Duration window = Duration.ofMinutes(1);
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 60000L;
		ath.generateItems(cs, 100, start_time, time_delta, "0.01", "0.01", "1", "1");
		start_time = start_time / window.toMillis() * window.toMillis(); // make it round

		RequestSpecification spec = ath.getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		waitUntil(() -> {
				TuplesResponseDTO response = ath.apiGetTuples(spec, "M1", cs.symbol);
				assertNotError(response);
				return response.data.rows.size() == 100 ? r.complete(response) : false;
			}, Duration.ofMinutes(2L));
		TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(100, actual_rows.size());
		assertEquals(new Tuple(start_time +           0L, "0.01", "0.01", "0.01", "0.01", "1"), actual_rows.get(0));
		assertEquals(new Tuple(start_time + 60000L * 99L, "1.00", "1.00", "1.00", "1.00", "100"), actual_rows.get(99));
		List<Tuple> expected_rows = new ArrayList<>();
		for ( long i = 0; i < 100; i ++ ) {
			long exp_time = start_time + 60000L * i;
			BigDecimal exp_val = new BigDecimal("0.01").multiply(new BigDecimal(i + 1L));
			BigDecimal exp_volume = BigDecimal.ONE.multiply(new BigDecimal(i + 1L));
			expected_rows.add(new Tuple(exp_time, exp_val, exp_val, exp_val, exp_val, exp_volume));
		}
		assertEqualsTupleByTuple("Test failed", expected_rows, actual_rows);
	}
	
	@Test
	public void C9002_Tuples_FromTimeToTime() throws Exception {
		Duration window = Duration.ofMinutes(1);
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 60000L;
		ath.generateItems(cs, 100, start_time, time_delta, "0.01", "0.01", "1", "1");
		start_time = start_time / window.toMillis() * window.toMillis();
		
		RequestSpecification spec = ath.getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		final long _tb = start_time;
		waitUntil(() -> {
				TuplesResponseDTO response = ath.apiGetTuples(spec, "M1", cs.symbol, null, _tb+1200000L, _tb+4800000L);
				assertNotError(response);
				return response.data.rows.size() == 60 ? r.complete(response) : false;
			}, Duration.ofMinutes(2L));
		TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(60, actual_rows.size());
		assertEquals(new Tuple(start_time + 1200000, "0.21", "0.21", "0.21", "0.21", "21"), actual_rows.get(0));
		assertEquals(new Tuple(start_time + 4740000, "0.80", "0.80", "0.80", "0.80", "80"), actual_rows.get(59));
		BigDecimal init_val = new BigDecimal("0.21"), init_vol = new BigDecimal("21"),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( int i = 0; i < 60; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(start_time + 1200000 + 60000 * i,
					val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		assertEqualsTupleByTuple("Test failed", expected_rows, actual_rows);
	}
	
	@Test
	public void C9003_Tuples_LimitHasHigherPriorityThanTimeTo() throws Exception {
		Duration window = Duration.ofMinutes(1L);
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 60000L;
		ath.generateItems(cs, 100, start_time, time_delta, "0.01", "0.01", "1", "1");
		start_time = start_time / window.toMillis() * window.toMillis();
		
		RequestSpecification spec = ath.getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		final long _tb = start_time;
		waitUntil(() -> {
				TuplesResponseDTO response = ath.apiGetTuples(spec, "M1", cs.symbol, 20, _tb+1200000L, _tb+4800000L);
				assertNotError(response);
				return response.data.rows.size() == 20 ? r.complete(response) : false;
			}, Duration.ofMinutes(2L));
		TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(20, actual_rows.size());
		assertEquals(new Tuple(start_time + 1200000L, "0.21", "0.21", "0.21", "0.21", "21"), actual_rows.get(0));
		assertEquals(new Tuple(start_time + 2340000L, "0.40", "0.40", "0.40", "0.40", "40"), actual_rows.get(19));
		BigDecimal init_val = new BigDecimal("0.21"), init_vol = new BigDecimal("21"),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( long i = 0; i < 20; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(start_time + 1200000L + 60000L * i,
					val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		assertEqualsTupleByTuple("Test failed", expected_rows, actual_rows);
	}
	
	@Test
	public void C9004_Tuples_ShouldUseMaxLimitIfRequestedLimitIsGreaterThanMaxLimit() throws Exception {
		Duration window = Duration.ofMinutes(1L);
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 60000L;
		ath.generateItems(cs, 7000, start_time, time_delta, "0.01", "0.01", "1", "1");
		start_time = start_time / window.toMillis() * window.toMillis();
		
		RequestSpecification spec = ath.getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		waitUntil(() -> {
				TuplesResponseDTO response = ath.apiGetTuples(spec, "M1", cs.symbol, 6000, null, null);
				assertNotError(response);
				return response.data.rows.size() == 5000 ? r.complete(response) : false;
			}, Duration.ofMinutes(1));
		TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(5000, actual_rows.size());
		assertEquals(new Tuple(start_time +         0L,  "0.01",  "0.01",  "0.01",  "0.01",  "1"), actual_rows.get(0));
		assertEquals(new Tuple(start_time + 299940000L, "50.00", "50.00", "50.00", "50.00", "5000"), actual_rows.get(4999));
		BigDecimal init_val = new BigDecimal("0.01"), init_vol = BigDecimal.ONE,
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( long i = 0; i < 5000; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(start_time + 60000L * i, val, val, val, val,
					delt_vol.multiply(mult).add(init_vol)));
		}
		assertEqualsTupleByTuple("Test failed", expected_rows, actual_rows);
	}
	
	@Test
	public void C9005_Tuples_AnyBacknodeShouldProvideDataAssociatedWithAnySymbol() throws Exception {
		Map<Integer, List<CatSym>> part_symbols = ath.newSymbolsOfDifferentPartitions(4);
		Iterator<Map.Entry<Integer, List<CatSym>>> it = part_symbols.entrySet().iterator();
		List<CatSym> cs_list = new ArrayList<>();
		while ( it.hasNext() ) {
			Map.Entry<Integer, List<CatSym>> entry = it.next();
			cs_list.addAll(entry.getValue());
		}
		Duration window = Duration.ofMinutes(1L);
		long start_time = ath.getRecentItemTimePlus1();
		ath.generateItems(cs_list, 7000, start_time, 60000L, "0.01", "0.01", "1", "1");
		start_time = start_time / window.toMillis() * window.toMillis();
		
		BigDecimal init_val = new BigDecimal("10.01"), init_vol = new BigDecimal(1001),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( long i = 0; i < 5000; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(start_time + 60000L * i + 60000000L, val, val, val, val,
					delt_vol.multiply(mult).add(init_vol)));
		}
		final long _tb = start_time;
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			for ( CatSym cs : cs_list ) {
				CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
				waitUntil(() -> {
						TuplesResponseDTO response = ath.apiGetTuples(spec, "M1", cs.symbol, null, _tb+60000000L, null);
						assertNotError(response);
						return response.data.rows.size() == 5000 ? r.complete(response) : false;
					}, Duration.ofMinutes(2L));
				TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
				assertEquals(cs.symbol, response.data.symbol);
				assertEquals("M1", response.data.period);
				assertEquals("std", response.data.format);
				List<Tuple> actual_rows = toTuples(response.data.rows);
				assertEqualsTupleByTuple(cs.toString(), expected_rows, actual_rows);
			}
		}
	}
	
	@Test
	public void C9006_Tuples_AnyBacknodeShouldProvideDataOfAllPeriods() throws Exception {
		Map<String, Duration> map = new LinkedHashMap<>();
		map.put("M1", Duration.ofMinutes(1));
		map.put("M2", Duration.ofMinutes(2));
		map.put("M3", Duration.ofMinutes(3));
		map.put("M5", Duration.ofMinutes(5));
		map.put("M6", Duration.ofMinutes(6));
		map.put("M10", Duration.ofMinutes(10));
		map.put("M12", Duration.ofMinutes(12));
		map.put("M15", Duration.ofMinutes(15));
		map.put("M20", Duration.ofMinutes(20));
		map.put("M30", Duration.ofMinutes(30));
		map.put("H1", Duration.ofHours(1));
		map.put("H2", Duration.ofHours(2));
		map.put("H3", Duration.ofHours(3));
		map.put("H4", Duration.ofHours(4));
		map.put("H6", Duration.ofHours(6));
		map.put("H8", Duration.ofHours(8));
		map.put("H12", Duration.ofHours(12));
		map.put("D1", Duration.ofDays(1));
		CatSym cs = ath.newSymbol();
		long start_time = ath.getRecentItemTimePlus1();
		long time_delta = 30000;
		int total_items = 5 * 24 * 60 * 60000 / (int)time_delta;
		ath.generateItems(cs, total_items, start_time, time_delta, "0.01", "0.01", "1", "1");

		for ( String period : map.keySet() ) {
			List<Tuple> expected_rows = ath.registeredItemsToTuples(cs, map.get(period).toMillis());
			if ( expected_rows.size() > 5000 ) { 
				expected_rows = expected_rows.subList(0, 5000);
			}
			for ( RequestSpecification spec : ath.getSpecAll() ) {
				int expected_count = expected_rows.size();
				CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
				waitUntil(() -> {
						TuplesResponseDTO response = ath.apiGetTuples(spec, period, cs.symbol);
						assertNotError(response);
						return response.data.rows.size() == expected_count ? r.complete(response) : false;
					}, Duration.ofMinutes(2L));
				TuplesResponseDTO response = r.get(1, TimeUnit.SECONDS);
				assertEquals(cs.symbol, response.data.symbol);
				assertEquals(period, response.data.period);
				assertEquals("std", response.data.format);
				List<Tuple> actual_rows = toTuples(response.data.rows);
				assertEqualsTupleByTuple(period, expected_rows, actual_rows);
			}
		}
	}
	
	@Test
	public void C9101_Periods() throws Exception {
		List<String> expected = Arrays.asList(
				"M1",
				"M2",
				"M3",
				"M5",
				"M6",
				"M10",
				"M12",
				"M15",
				"M20",
				"M30",
				"H1",
				"H2",
				"H3",
				"H4",
				"H6",
				"H8",
				"H12",
				"D1"
			);
		
		for ( RequestSpecification spec : ath.getSpecAll() ) {
			PeriodsResponseDTO response = ath.apiGetPeriods(spec);
			
			assertNotError(response);
			assertEquals(expected, response.data.rows);
		}
		
	}

}
