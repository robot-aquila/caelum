package ru.prolib.caelum.test;

import static org.junit.Assert.*;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.restassured.specification.RequestSpecification;
import ru.prolib.caelum.test.dto.CategoriesResponseDTO;
import ru.prolib.caelum.test.dto.ItemResponseDTO;
import ru.prolib.caelum.test.dto.ItemsResponseDTO;
import ru.prolib.caelum.test.dto.PingResponseDTO;
import ru.prolib.caelum.test.dto.SymbolResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdateDTO;
import ru.prolib.caelum.test.dto.SymbolUpdateResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdatesResponseDTO;
import ru.prolib.caelum.test.dto.SymbolsResponseDTO;
import ru.prolib.caelum.test.dto.TuplesResponseDTO;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BacknodeIT extends TestBasis {
	
	public BacknodeIT() {
		super(Arrays.asList("localhost:9698"));
	}
	
	@Before
	@Override
	public void setUp() {
		super.setUp();
	}
	
	@After
	@Override
	public void tearDown() {
		super.tearDown();
	}
	
	@Test
	public void C0000_Ping() {
		for ( RequestSpecification spec : getSpecAll() ) {
			PingResponseDTO response = apiPing(spec);
			assertNotError(response);
			assertNull(response.data);
		}
	}
	
	// C1*** - Test Cases of features related to items
	
	@Test
	public void C1001_PutItem_PutAndTestResponse() {
		CatSym cs = newSymbol();
		Item item1 = registerItem(cs, 10000L);

		ItemResponseDTO response = apiPutItem(getSpecRandom(), item1);
		
		assertNotError(response);
	}
	
	@Test
	public void C1002_PutItem_PutOfSameTimeAndTestSequence() {
		CatSym cs = newSymbol();
		assertNotError(apiPutItem(getSpecRandom(), registerItem(cs, 10000L)));
		assertNotError(apiPutItem(getSpecRandom(), registerItem(cs, 10000L)));
		assertNotError(apiPutItem(getSpecRandom(), registerItem(cs, 10000L)));
		assertNotError(apiPutItem(getSpecRandom(), registerItem(cs, 10000L)));
		
		ItemsResponseDTO response = apiGetItems(cs.symbol);
	
		assertNotError(response);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("std", response.data.format);
		assertNotNull(response.data.magic);
		assertThat(response.data.magic.length(), is(greaterThanOrEqualTo(1)));
		assertEquals(4, response.data.rows.size());
		assertEquals(registeredItems(cs), toItems(cs, response.data.rows));
	}
	
	@Test
	public void C1003_PutItem_ItemsOfDifferentSymbolsShouldBeOkTogether() {
		long time = 10000L;
		List<CatSym> symbols = new ArrayList<>();
		for ( int i = 1; i < 7; i ++ ) {
			CatSym cs = newSymbol();
			symbols.add(cs);
			for ( int j = 0; j < i; j ++ ) {
				assertNotError(apiPutItem(getSpecRandom(), registerItem(cs, time)));
				time ++;
			}
		}
		
		for ( int i = 0; i < symbols.size(); i ++ ) {
			CatSym cs = symbols.get(i);
			ItemsResponseDTO response = apiGetItems(cs.symbol);
			assertEquals(i + 1, response.data.rows.size());
			assertEquals(registeredItems(cs), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1004_PutItem_ShouldRegisterCategories() {
		for ( CatSym cs : newSymbols(5, 10) ) {
			apiPutItem(getSpecRandom(), registerItem(cs, 1000L));
		}
		
		for ( RequestSpecification spec : getSpecAll() ) {
			CategoriesResponseDTO response = apiGetCategories(spec);
			
			assertNotError(response);
			assertEquals(registeredCategories(), response.data.rows);
		}
	}
	
	@Test
	public void C1005_PutItem_ShouldRegisterSymbols() throws Exception {
		long time = 10000L;
		for ( CatSym cs : newSymbols(5, 10) ) {
			apiPutItem(getSpecRandom(), registerItem(cs, time ++));
		}
		
		for ( RequestSpecification spec : getSpecAll() ) {
			for ( String category : registeredCategories() ) {
				final List<String> expected_symbols = registeredSymbols(category);
				CompletableFuture<SymbolsResponseDTO> r = new CompletableFuture<>();
				await().dontCatchUncaughtExceptions()
					.pollInterval(Duration.ofSeconds(5L))
					.atMost(Duration.ofSeconds(20))
					.until(() -> {
						SymbolsResponseDTO response = apiGetSymbols(spec, category);
						assertNotError(response);
						if ( response.data.rows.size() == expected_symbols.size() ) {
							r.complete(response);
							return true;
						} else {
							return false;
						}
					});
				SymbolsResponseDTO response = r.get(1, TimeUnit.SECONDS);
				
				assertNotError(response);
				assertEquals(registeredSymbols(category), response.data.rows);
			}
		}
	}
	
	@Test
	public void C1006_PutItem_BatchModeShouldWorkOk() {
		long time = 100000L;
		CatSym cs1 = newSymbol(), cs2 = newSymbol();
		List<Item> items = new ArrayList<>();
		for ( int i = 0; i < 50; i ++ ) {
			items.add(registerItem(cs1, time));
			items.add(registerItem(cs2, time ++));
		}
		
		assertNotError(apiPutItem(getSpecRandom(), items));
		
		ItemsResponseDTO response;
		for ( RequestSpecification spec : getSpecAll() ) {
			assertNotError(response = apiGetItems(spec, cs1.symbol, null));
			assertEquals(50, response.data.rows.size());
			assertEquals(registeredItems(cs1), toItems(cs1, response.data.rows));
			
			assertNotError(response = apiGetItems(spec, cs2.symbol, null));
			assertEquals(50, response.data.rows.size());
			assertEquals(registeredItems(cs2), toItems(cs2, response.data.rows));
		}
	}

	@Test
	public void C1050_Items_AllShouldBeLimitedUpTo5000() throws Exception {
		CatSym cs = newSymbol();
		long time_delta = 30000L; // +30 seconds for each
		int total_m1_tuples = 4 * 24 * 60, total_items = (int) (total_m1_tuples * 60000 / time_delta);
		assertThat(total_items, is(greaterThan(5000 * 2)));
		generateItems(cs, total_items, 10000L, time_delta, "0.001", "0.001", "5", "5");

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null);
			assertNotError(response);
			assertEquals(5000, response.data.rows.size());
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);	
			assertThat(response.data.fromOffset, is(greaterThanOrEqualTo(5000L)));
			assertNotNull(response.data.magic);
			assertThat(response.data.magic.length(), is(equalTo(32)));
			assertEquals(registeredItems(cs).subList(0,  5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1051_Items_WithLimitLessThanMaxLimitAndLessThanItemsCount() {
		CatSym cs = newSymbol();
		generateItems(cs, 800, 129800L, 15000L, "0.050", "0.025", "1", "1");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 500);
			
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(500, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(registeredItems(cs).subList(0, 500), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1052_Items_WithLimitLessThanMaxLimitButGreaterThanItemsCount() {
		CatSym cs = newSymbol();
		generateItems(cs, 800, 130999L, 15000L, "0.050", "0.025", "1", "2");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 1000);
			
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(800, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(registeredItems(cs).subList(0, 800), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1053_Items_WithLimitGreaterThanMaxLimitAndLessThanItemsCount() {
		CatSym cs = newSymbol();
		generateItems(cs, 5500, 178896L, 15000L, "1025.0", "0.5", "1000", "10");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 5200);
			
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(5000, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(5500, registeredItems(cs).size());
			assertEquals(registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1054_Items_WithLimitGreaterThanMaxLimitAndGreaterThanItemsCount() {
		CatSym cs = newSymbol();
		generateItems(cs, 5500, 172991L, 15000L, "100.24919", "0.00001", "10900", "-1");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 7000);
			
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(5000, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			assertEquals(5500, registeredItems(cs).size());
			assertEquals(registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1055_Items_ShouldConsiderTimeFromInclusiveAndTimeToExclusive() {
		CatSym cs = newSymbol();
		generateItems(cs, 1000, 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem( 200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem( 300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(1099000L, "0.15505",  "900"), expected.get(899));
		assertEquals(                 cs.newItem(1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 900);

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null, 300000L, 1100000L);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(800, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1056_Items_ShouldConsiderTimeFromInclusive() {
		CatSym cs = newSymbol();
		generateItems(cs, 1000, 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem( 200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem( 300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 1000);
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null, 300000L, null);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(900, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1057_Items_ShouldConsiderTimeToExclusive() {
		CatSym cs = newSymbol();
		generateItems(cs, 1000, 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(expected_first = cs.newItem( 200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_last  = cs.newItem(1099000L, "0.15505",  "900"), expected.get(899));
		assertEquals(                 cs.newItem(1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(000, 900);

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null, null, 1100000L);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(900, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1058_Items_LimitShouldHaveGreaterPriorityThanTimeTo() {
		CatSym cs = newSymbol();
		generateItems(cs, 1000, 200000L, 1000L, "0.20000", "-0.00005", "1", "1");
		List<Item> expected = registeredItems(cs);
		Item expected_first, expected_last;
		assertEquals(                 cs.newItem( 200000L, "0.20000",    "1"), expected.get(  0));
		assertEquals(expected_first = cs.newItem( 300000L, "0.19500",  "101"), expected.get(100));
		assertEquals(expected_last  = cs.newItem(1049000L, "0.15755",  "850"), expected.get(849));
		assertEquals(                 cs.newItem(1199000L, "0.15005", "1000"), expected.get(999));
		expected = expected.subList(100, 850);

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 750, 300000L, 1100000L);

			List<Item> actual = toItems(cs, response.data.rows);
			assertEquals(expected_first, actual.get(0));
			assertEquals(expected_last, actual.get(actual.size() - 1));
			assertEquals(750, actual.size());
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1059_Items_ShouldConsiderFromOffset() {
		CatSym cs = newSymbol();
		generateItems(cs, 12000, 230000L, 1000L, "1.0000", "0.0005", "1", "1");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null);
			assertEquals(registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
			
			response = apiGetItems(spec, cs.symbol, null, null, null, response.data.magic, response.data.fromOffset);
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertNotNull(response.data.magic);
			assertThat(response.data.magic.length(), is(greaterThan(0)));
			assertNotNull(response.data.fromOffset);
			assertEquals(registeredItems(cs).subList(5000, 10000), toItems(cs, response.data.rows));
			
			response = apiGetItems(spec, cs.symbol, null, null, null, response.data.magic, response.data.fromOffset);
			assertNotError(response);
			assertEquals(cs.symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertNotNull(response.data.magic);
			assertThat(response.data.magic.length(), is(greaterThan(0)));
			assertNotNull(response.data.fromOffset);
			assertEquals(registeredItems(cs).subList(10000, 12000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1060_Items_ShouldIgnoreTimeFromIfFromOffsetSpecified() {
		CatSym cs = newSymbol();
		generateItems(cs, 6000, 230000L, 1000L, "1.0000", "0.0005", "1", "1");

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null);
			assertEquals(registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
			
			response = apiGetItems(spec, cs.symbol, null, 730000L, null, response.data.magic, response.data.fromOffset);
			assertNotError(response);
			assertEquals(registeredItems(cs).subList(5000, 6000), toItems(cs, response.data.rows));
		}
	}
	
	@Test
	public void C1061_Items_ShouldBeOkIfOutOfRangeIfFromOffsetSpecified() {
		CatSym cs = newSymbol();
		generateItems(cs, 6000, 230000L, 1000L, "1.0000", "0.0005", "1", "1");

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null);
			assertEquals(registeredItems(cs).subList(0, 5000), toItems(cs, response.data.rows));
			
			response = apiGetItems(spec, cs.symbol, 1, null, null, response.data.magic, 1128276L);
			assertNotError(response);
			assertEquals(0, response.data.rows.size());
		}
	}	
	
	@Test
	public void C1062_Items_ConcurrentRequestsShouldGiveSameResults() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 6000);
		int num_threads = 5;
		CountDownLatch started = new CountDownLatch(num_threads),
			go = new CountDownLatch(1), finished = new CountDownLatch(num_threads);
		RequestSpecification spec = getSpecRandom();
		List<CompletableFuture<ItemsResponseDTO>> result = new ArrayList<>();
		for ( int i = 0; i < num_threads; i ++ ) {
			final CompletableFuture<ItemsResponseDTO> f = new CompletableFuture<>();
			new Thread() {
				@Override
				public void run() {
					try {
						started.countDown();
						if ( go.await(5, TimeUnit.SECONDS) ) {
							ItemsResponseDTO response = apiGetItems(spec, cs.symbol, null);
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
		List<Item> expected = registeredItems(cs).subList(0, 5000);
		for ( int i = 0; i < num_threads; i ++ ) {
			CompletableFuture<ItemsResponseDTO> f = result.get(i);
			assertEqualsItemByItem("Thread #" + i, expected, toItems(cs, f.get().data.rows));
		}
	}

	@Test
	public void C1063_Items_TwoConsecutiveCallsShouldGiveSameResult() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 4000);

		for ( RequestSpecification spec : getSpecAll() ) {
			CompletableFuture<ItemsResponseDTO> r = new CompletableFuture<>();
			await().dontCatchUncaughtExceptions()
				.pollInterval(Duration.ofSeconds(5L))
				.atMost(Duration.ofSeconds(20))
				.until(() -> {
					ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 3000);
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
			assertNotError(response2 = apiGetItems(spec, cs.symbol, 3000));
			
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
		Map<Integer, List<CatSym>> map = newSymbolsOfDifferentPartitions(5);
		for ( int num_partitions : map.keySet() ) {
			for ( CatSym cs : map.get(num_partitions) ) {
				generateItems(cs, 1000);
			}
		}
		
		for ( RequestSpecification spec : getSpecAll() ) {
			for ( int num_partitions : map.keySet() ) {
				for ( CatSym cs : map.get(num_partitions) ) {
					ItemsResponseDTO response = apiGetItems(spec, cs.symbol, 877);
					assertNotError(response);
					assertEquals(registeredItems(cs).subList(0,  877), toItems(cs, response.data.rows));
				}
			}	
		}
	}
	
	// C2*** - Test Cases of features related to symbol and symbol updates
	
	@Test
	public void C2010_PutSymbol_PutAndTestResponse() {
		CatSym cs = newSymbol();

		SymbolResponseDTO response = apiPutSymbol(getSpecRandom(), cs.symbol);
		
		assertNotError(response);
	}
	
	@Test
	public void C2011_PutSymbol_ShouldBeOkIfEmptyCategory() {
		CatSym cs = newSymbol("");
		
		assertNotError(apiPutSymbol(getSpecRandom(), cs.symbol));
		
		CategoriesResponseDTO response = apiGetCategories();
		assertNotError(response);
		assertEquals(Arrays.asList(""), response.data.rows);
	}
	
	@Test
	public void C2012_PutSymbol_ShouldRegisterCategory() {
		List<CatSym> symbols = newSymbols(2, 1);
		CatSym cs1 = symbols.get(0), cs2 = symbols.get(1);
		
		assertNotError(apiPutSymbol(getSpecRandom(), cs1.symbol));
		assertNotError(apiPutSymbol(getSpecRandom(), cs2.symbol));
		
		CategoriesResponseDTO response = apiGetCategories();
		assertNotError(response);
		List<String> expected = new ArrayList<>(Arrays.asList(cs1.category, cs2.category));
		Collections.sort(expected);
		assertEquals(expected, response.data.rows);
	}
	
	@Test
	public void C2012_PutSymbol_ShouldSupportBatchMode() {
		List<CatSym> symbols = newSymbols(1, 20);
		
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		SymbolsResponseDTO response = apiGetSymbols(getSpecRandom(), category);
		assertNotError(response);
		assertEquals(20, response.data.rows.size());
		assertEquals(registeredSymbols(category), response.data.rows);
	}
	
	@Test
	public void C2030_Symbols_GetAll() {
		List<CatSym> symbols = newSymbols(10, 20);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		List<String> categories = registeredCategories();
		assertEquals(10, categories.size());
		for ( RequestSpecification spec : getSpecAll() ) {
			for ( String category : categories ) {
				SymbolsResponseDTO response = apiGetSymbols(spec, category);
				assertNotError(response);
				assertEquals(registeredSymbols(category), response.data.rows);
				assertEquals(20, response.data.rows.size());
			}
		}
	}
	
	@Test
	public void C2031_Symbols_ShouldApplyDefaultLimit() {
		List<CatSym> symbols = newSymbols(1, 7000);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolsResponseDTO response = apiGetSymbols(spec, category);
			assertNotError(response);
			assertEquals(registeredSymbols(category).subList(0, 5000), response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}
	
	@Test
	public void C2032_Symbols_WithLimitGreaterThanMaxLimit() {
		List<CatSym> symbols = newSymbols(1, 7000);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolsResponseDTO response = apiGetSymbols(spec, category, 5100);
			assertNotError(response);
			assertEquals(registeredSymbols(category).subList(0, 5000), response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}
	
	@Test
	public void C2033_Symbols_WithLimitGreaterThanMaxLimitAndAfterSymbol() {
		List<CatSym> symbols = newSymbols(1, 7000);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		String category = symbols.get(0).category;
		List<String> expected = registeredSymbols(category);
		String afterSymbol = expected.get(199);
		expected = expected.subList(200, 5200);
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolsResponseDTO response = apiGetSymbols(spec, category, afterSymbol, 8000);
			assertNotError(response);
			assertEquals(expected, response.data.rows);
			assertEquals(5000, response.data.rows.size());
		}
	}

	@Test
	public void C2034_Symbols_WithLimit() {
		List<CatSym> symbols = newSymbols(5, 20);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));
		
		List<String> expected;
		SymbolsResponseDTO response;
		
		String expected_category = symbols.get(0).category;
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, expected_category, 5);
			assertNotError(response);
			
			expected = registeredSymbols(expected_category).subList(0,  5);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(5, response.data.rows.size());
		}
	}
	
	@Test
	public void C2035_Symbols_WithAfterSymbol() {
		List<CatSym> symbols = newSymbols(5, 20);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));

		List<String> expected;
		SymbolsResponseDTO response;
		
		String expected_category = symbols.get(0).category;
		String after_symbol = registeredSymbols(expected_category).get(2);
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, expected_category, after_symbol); // 3-19
			assertNotError(response);
			
			expected = registeredSymbols(expected_category).subList(3, 20);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(17, response.data.rows.size());
		}
	}
	
	@Test
	public void C2036_Symbols_WithLimitAndAfterSymbol() {
		List<CatSym> symbols = newSymbols(5, 20);
		assertNotError(apiPutSymbolCS(getSpecRandom(), symbols));

		List<String> expected;
		SymbolsResponseDTO response;
		String expected_category = symbols.get(0).category;
		String after_symbol = registeredSymbols(expected_category).get(2);
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, expected_category, after_symbol, 15);
			assertNotError(response);
			
			expected = registeredSymbols(expected_category).subList(3, 18);
			assertEquals(expected, response.data.rows);
			assertEquals(expected_category, response.data.category);
			assertEquals(15, response.data.rows.size());
		}
	}
	
	@Test
	public void C2050_PutSymbolUpdate_PutAndTestResponse() {
		CatSym cs = newSymbol();
		
		SymbolUpdateResponseDTO response =
				apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 112456L, 1, "foo", 2, "bar", 5, "buzz");
		
		assertNotError(response);
	}
	
	@Test
	public void C2051_PutSymbolUpdate_ShouldRegisterUpdate() {
		CatSym cs = newSymbol();
		
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 237991L, 50, "pop", 51, "gap", 52, "die"));

		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolUpdatesResponseDTO response = apiGetSymbolUpdates(spec, cs.symbol);
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
		CatSym cs = newSymbol();
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279390L, 1, "foo"));
		
		for ( RequestSpecification spec : getSpecAll() ) {
			CategoriesResponseDTO response = apiGetCategories(spec);
			assertNotError(response);
			assertEquals(Arrays.asList(cs.category), response.data.rows);
		}
	}
	
	@Test
	public void C2053_PutSymbolUpdate_ShouldRegisterSymbol() {
		CatSym cs = newSymbol();
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279390L, 1, "foo"));

		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolsResponseDTO response = apiGetSymbols(spec, cs.category);
			assertNotError(response);
			assertEquals(Arrays.asList(cs.symbol), response.data.rows);
		}
	}

	@Test
	public void C2054_PutSymbolUpdate_ShouldOverrideExistingUpdate() {
		CatSym cs = newSymbol();
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279390L, 1, "foo"));
		
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279390L, 5, "back", 7, "rogers"));
		
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolUpdatesResponseDTO response = apiGetSymbolUpdates(spec, cs.symbol);
			assertNotError(response);
			List<SymbolUpdateDTO> expected = Arrays.asList(
					new SymbolUpdateDTO(279390L, toMap(5, "back", 7, "rogers"))
				);
			assertEquals(expected, response.data.rows);
		}
	}
	
	@Test
	public void C2060_SymbolUpdates() {
		CatSym cs = newSymbol();
		// the order does not matter
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279200L, 5, "back", 7, "rogers"));
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279100L, 1, "foo"));
		assertNotError(apiPutSymbolUpdate(getSpecRandom(), cs.symbol, 279000L, 3, "mamba", 4, "garpia"));
		
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolUpdatesResponseDTO response = apiGetSymbolUpdates(spec, cs.symbol);
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
		assertNotError(apiPutSymbol(getSpecRandom(), Arrays.asList("kobresia", "canopus", "foo@sirius", "bar@io")));
		
		for ( RequestSpecification spec : getSpecAll() ) {
			SymbolsResponseDTO response = apiGetSymbols(spec, "");
			assertNotError(response);
			assertEquals(Arrays.asList("canopus", "kobresia"), response.data.rows);
		}
	}
	
	@Test
	public void C3002_Categories_ShouldReturnAllCategoriesIncludingEmpty() {
		assertNotError(apiPutSymbol(getSpecRandom(), Arrays.asList("kobresia", "canopus", "foo@sirius", "bar@io")));
		
		for ( RequestSpecification spec : getSpecAll() ) {
			CategoriesResponseDTO response = apiGetCategories(spec);
			assertNotError(response);
			assertEquals(Arrays.asList("", "bar", "foo"), response.data.rows);
		}
	}
	
	// C9*** - Test Cases of features related to tuples
	
	@Test
	public void C9001_Tuples_SimpleM1Request() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 100, 1L, 60000L, "0.01", "0.01", "1", "1");

		RequestSpecification spec = getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		await().dontCatchUncaughtExceptions()
			.pollInterval(Duration.ofSeconds(10L))
			.atMost(Duration.ofSeconds(20))
			.until(() -> {
				TuplesResponseDTO response = apiGetTuples(spec, "M1", cs.symbol);
				assertNotError(response);
				if ( response.data.rows.size() == 100 ) {
					r.complete(response);
					return true;
				} else {
					return false;
				}
			});
		TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(100, actual_rows.size());
		assertEquals(new Tuple(0L, "0.01", "0.01", "0.01", "0.01", "1"), actual_rows.get(0));
		assertEquals(new Tuple(60000 * 99, "1.00", "1.00", "1.00", "1.00", "100"), actual_rows.get(99));
		for ( int i = 0; i < 100; i ++ ) {
			long expected_time = 60000 * i;
			BigDecimal expected_val = new BigDecimal("0.01").multiply(new BigDecimal(i + 1));
			BigDecimal expected_vol = BigDecimal.ONE.multiply(new BigDecimal(i + 1));
			assertEquals("Tuple #" + i,
				new Tuple(expected_time, expected_val, expected_val, expected_val, expected_val, expected_vol),
				actual_rows.get(i));
		}
	}
	
	@Test
	public void C9002_Tuples_FromTimeToTime() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 100, 1L, 60000L, "0.01", "0.01", "1", "1");
		
		RequestSpecification spec = getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		await().dontCatchUncaughtExceptions()
			.pollInterval(Duration.ofSeconds(10L))
			.atMost(Duration.ofSeconds(20))
			.until(() -> {
				TuplesResponseDTO response = apiGetTuples(spec, "M1", cs.symbol, null, 1200000L, 4800000L);
				assertNotError(response);
				if ( response.data.rows.size() == 60 ) {
					r.complete(response);
					return true;
				} else {
					return false;
				}
			});
		TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(60, actual_rows.size());
		assertEquals(new Tuple(1200000, "0.21", "0.21", "0.21", "0.21", "21"), actual_rows.get(0));
		assertEquals(new Tuple(4740000, "0.80", "0.80", "0.80", "0.80", "80"), actual_rows.get(59));
		BigDecimal init_val = new BigDecimal("0.21"), init_vol = new BigDecimal("21"),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( int i = 0; i < 60; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(1200000 + 60000 * i,
					val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		assertEquals(expected_rows, actual_rows);
	}
	
	@Test
	public void C9003_Tuples_LimitHasHigherPriorityThanTimeTo() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 100, 1L, 60000L, "0.01", "0.01", "1", "1");
		
		RequestSpecification spec = getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		await().dontCatchUncaughtExceptions()
			.pollInterval(Duration.ofSeconds(5L))
			.atMost(Duration.ofSeconds(20))
			.until(() -> {
				TuplesResponseDTO response = apiGetTuples(spec, "M1", cs.symbol, 20, 1200000L, 4800000L);
				assertNotError(response);
				if ( response.data.rows.size() == 20 ) {
					r.complete(response);
					return true;
				} else {
					return false;
				}
			});
		TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(20, actual_rows.size());
		assertEquals(new Tuple(1200000, "0.21", "0.21", "0.21", "0.21", "21"), actual_rows.get(0));
		assertEquals(new Tuple(2340000, "0.40", "0.40", "0.40", "0.40", "40"), actual_rows.get(19));
		BigDecimal init_val = new BigDecimal("0.21"), init_vol = new BigDecimal("21"),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( int i = 0; i < 20; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(1200000 + 60000 * i,
					val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		assertEquals(expected_rows, actual_rows);
	}
	
	@Test
	public void C9004_Tuples_ShouldUseMaxLimitIfRequestedLimitIsGreaterThanMaxLimit() throws Exception {
		CatSym cs = newSymbol();
		generateItems(cs, 7000, 1L, 60000L, "0.01", "0.01", "1", "1");
		
		RequestSpecification spec = getSpecRandom();
		CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
		await().dontCatchUncaughtExceptions()
			.pollInterval(Duration.ofSeconds(5L))
			.atMost(Duration.ofSeconds(20))
			.until(() -> {
				TuplesResponseDTO response = apiGetTuples(spec, "M1", cs.symbol, 6000, null, null);
				assertNotError(response);
				if ( response.data.rows.size() == 5000 ) {
					r.complete(response);
					return true;
				} else {
					return false;
				}
			});
		TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
		assertEquals(cs.symbol, response.data.symbol);
		assertEquals("M1", response.data.period);
		assertEquals("std", response.data.format);
		List<Tuple> actual_rows = toTuples(response.data.rows);
		assertEquals(5000, actual_rows.size());
		assertEquals(new Tuple(        0,  "0.01",  "0.01",  "0.01",  "0.01",  "1"), actual_rows.get(0));
		assertEquals(new Tuple(299940000, "50.00", "50.00", "50.00", "50.00", "5000"), actual_rows.get(4999));
		BigDecimal init_val = new BigDecimal("0.01"), init_vol = BigDecimal.ONE,
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( int i = 0; i < 5000; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(60000 * i, val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		assertEquals(expected_rows, actual_rows);
	}
	
	@Test
	public void C9005_Tuples_AnyBacknodeShouldProvideDataAssociatedWithAnySymbol() throws Exception {
		Map<Integer, List<CatSym>> part_symbols = newSymbolsOfDifferentPartitions(4);
		Iterator<Map.Entry<Integer, List<CatSym>>> it = part_symbols.entrySet().iterator();
		List<CatSym> cs_list = new ArrayList<>();
		while ( it.hasNext() ) {
			Map.Entry<Integer, List<CatSym>> entry = it.next();
			cs_list.addAll(entry.getValue());
		}
		generateItems(cs_list, 7000, 1L, 60000L, "0.01", "0.01", "1", "1");
		
		BigDecimal init_val = new BigDecimal("10.01"), init_vol = new BigDecimal(1001),
				delt_val = new BigDecimal("0.01"), delt_vol = BigDecimal.ONE;
		List<Tuple> expected_rows = new ArrayList<>();
		for ( int i = 0; i < 5000; i ++ ) {
			BigDecimal mult = new BigDecimal(i);
			BigDecimal val = delt_val.multiply(mult).add(init_val);
			expected_rows.add(new Tuple(60000 * i + 60000000, val, val, val, val, delt_vol.multiply(mult).add(init_vol)));
		}
		for ( RequestSpecification spec : getSpecAll() ) {
			for ( CatSym cs : cs_list ) {
				CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
				await().dontCatchUncaughtExceptions()
					.pollInterval(Duration.ofSeconds(5L))
					.atMost(Duration.ofSeconds(20))
					.until(() -> {
						TuplesResponseDTO response = apiGetTuples(spec, "M1", cs.symbol, null, 60000000L, null);
						assertNotError(response);
						if ( response.data.rows.size() == 5000 ) {
							r.complete(response);
							return true;
						} else {
							return false;
						}
					});
				TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
				assertEquals(cs.symbol, response.data.symbol);
				assertEquals("M1", response.data.period);
				assertEquals("std", response.data.format);
				List<Tuple> actual_rows = toTuples(response.data.rows);
				assertEquals(expected_rows, actual_rows);
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
		CatSym cs = newSymbol();
		long time_delta = 30000;
		int total_items = 5 * 24 * 60 * 60000 / (int)time_delta;
		generateItems(cs, total_items, 1, time_delta, "0.01", "0.01", "1", "1");

		for ( String period : map.keySet() ) {
			List<Tuple> expected_rows = registeredItemsToTuples(cs, map.get(period).toMillis());
			if ( expected_rows.size() > 5000 ) { 
				expected_rows = expected_rows.subList(0, 5000);
			}
			for ( RequestSpecification spec : getSpecAll() ) {
				int expected_count = expected_rows.size();
				CompletableFuture<TuplesResponseDTO> r = new CompletableFuture<>();
				await().dontCatchUncaughtExceptions()
					.pollInterval(Duration.ofSeconds(5L))
					.atMost(Duration.ofSeconds(20))
					.until(() -> {
						TuplesResponseDTO response = apiGetTuples(spec, period, cs.symbol);
						assertNotError(response);
						if ( response.data.rows.size() == expected_count ) {
							r.complete(response);
							return true;
						} else {
							return false;
						}
					});
				TuplesResponseDTO response = r.get(1, TimeUnit.MINUTES);
				assertEquals(cs.symbol, response.data.symbol);
				assertEquals(period, response.data.period);
				assertEquals("std", response.data.format);
				List<Tuple> actual_rows = toTuples(response.data.rows);
				assertEqualsTupleByTuple(period, expected_rows, actual_rows);
			}
		}
	}

}
