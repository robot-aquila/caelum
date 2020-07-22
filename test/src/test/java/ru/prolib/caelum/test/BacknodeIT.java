package ru.prolib.caelum.test;

import static org.junit.Assert.*;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BacknodeIT {
	public static final String CAT1 = "zoo", CAT2 = "old", CAT3 = "bar", CAT4 = "wow";
	public static final String CAT1_SYMBOL1 = "zoo@gap";
	public static final String CAT1_SYMBOL2 = "zoo@lol";
	public static final String CAT1_SYMBOL3 = "zoo@foo";
	public static final String CAT1_SYMBOL4 = "zoo@bar";
	public static final String CAT2_SYMBOL1 = "old@car";
	public static final String CAT2_SYMBOL2 = "old@get";
	public static final String CAT2_SYMBOL3 = "old@orb";
	public static final String CAT3_SYMBOL1 = "bar@dar";
	public static final String CAT3_SYMBOL2 = "bar@pop";
	public static final String CAT3_SYMBOL3 = "bar@var";
	public static final String CAT4_SYMBOL1 = "wow@gap";
	public static final String CAT4_SYMBOL2 = "wow@bob";
	
	public static class Item {
		final String category, symbol;
		final long time;
		final BigDecimal value, volume;
		
		public Item(String category, String symbol, long time, BigDecimal value, BigDecimal volume) {
			this.category = category;
			this.symbol = symbol;
			this.time = time;
			this.value = value;
			this.volume = volume;
		}
		
		public Item(String category, String symbol, long time, String value, String volume) {
			this(category, symbol, time, new BigDecimal(value), new BigDecimal(volume));
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("symbol", symbol)
					.append("time", time)
					.append("value", value)
					.append("volume", volume)
					.build();
		}
		
		@Override
		public int hashCode() {
			return new HashCodeBuilder(1179015, 837)
					.append(symbol)
					.append(time)
					.append(value)
					.append(volume)
					.build();
		}
		
		@Override
		public boolean equals(Object other) {
			if ( other == this ) {
				return true;
			}
			if ( other == null || other.getClass() != Item.class ) {
				return false;
			}
			Item o = (Item) other;
			return new EqualsBuilder()
					.append(o.symbol, symbol)
					.append(o.time, time)
					.append(o.value, value)
					.append(o.volume, volume)
					.build();
		}
		
	}
	
	public static class CatSym {
		private static final Random r = new Random();
		final String category, symbol;
		final byte decimals, volumeDecimals;
		
		public CatSym(String category, String symbol, byte decimals, byte volumeDecimals) {
			this.category = category;
			this.symbol = symbol;
			this.decimals = decimals;
			this.volumeDecimals = volumeDecimals;
		}
		
		BigDecimal toBD(long value, int scale) {
			return new BigDecimal(value).divide(BigDecimal.TEN.pow(scale));
		}
		
		/**
		 * Create new item of this symbol with random value and volume.
		 * <p>
		 * @param time - item time
		 * @return new item instance
		 */
		public Item newItem(long time) {
			return new Item(category, symbol, time, toBD(r.nextLong(), decimals), toBD(r.nextLong(), volumeDecimals));
		}
		
		public static CatSym random(String category, int comp_len) {
			return new CatSym(category,
					category + "@" + RandomStringUtils.random(comp_len, true, true),
					(byte) r.nextInt(16),
					(byte) r.nextInt(16));
		}
		
		public static CatSym random(int comp_len) {
			return random(RandomStringUtils.random(comp_len, true, true), comp_len);
			
		}
		
		public static CatSym random() {
			return random(6);
		}
		
		public static CatSym random(String category) {
			return random(category, 6);
		}
		
	}
	
	protected static final Map<String, List<Item>> databaseReplica = new HashMap<>();
	protected static final Set<String> existingSymbols = new HashSet<>();
	
	public static CatSym newSymbol(String category) {
		for ( int i = 0; i < 1000; i ++ ) {
			CatSym cs = CatSym.random(category);
			if ( ! existingSymbols.contains(cs.symbol) ) {
				existingSymbols.add(cs.symbol);
				return cs;
			}
		}
		throw new IllegalStateException();		
	}
	
	public static CatSym newSymbol() {
		for ( int i = 0; i < 1000; i ++ ) {
			CatSym cs = CatSym.random();
			if ( ! existingSymbols.contains(cs.symbol) ) {
				existingSymbols.add(cs.symbol);
				return cs;
			}
		}
		throw new IllegalStateException();
	}
	
	public static List<CatSym> newSymbols(int num_categories, int num_symbols) {
		assertThat(num_categories, is(greaterThan(0)));
		assertThat(num_symbols, is(greaterThan(0)));
		List<CatSym> result = new ArrayList<>();
		for ( int i = 0; i < num_categories; i ++ ) {
			CatSym first = newSymbol();
			result.add(first);
			for ( int j = 1; j < num_symbols; j ++ ) {
				result.add(newSymbol(first.category));
			}
		}
		return result;
	}
	
	public static Map<Integer, String> toMap(Object... args) {
		if ( args.length % 2 != 0 ) {
			throw new IllegalArgumentException();
		}
		Map<Integer, String> result = new LinkedHashMap<>();
		for ( int i = 0; i < args.length / 2; i ++ ) {
			Integer key = (Integer) args[i * 2];
			String val = (String) args[i * 2 + 1];
			result.put(key, val);
		}
		return result;
	}
	
	public static long timeDiff(ResponseDTO response) {
		return Math.abs(System.currentTimeMillis() - response.time);
	}
	
	public static void assertNotError(ResponseDTO response) {
		assertThat(timeDiff(response), is(lessThanOrEqualTo(1000L)));
		assertEquals(0, (int) response.code);
		assertNull(response.message);
		assertFalse(response.error);
	}
	
	public static void assertNotError(ItemResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNull(response.data);
	}
	
	public static void assertNotError(ItemsResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNotNull(response.data);
	}
	
	public static List<Item> toItems(String category, String symbol, List<List<Object>> rows) {
		List<Item> result = new ArrayList<>();
		for ( List<Object> row : rows ) {
			long time = 0;
			if ( row.get(0) instanceof Integer ) {
				time = (int) row.get(0);
			} else if ( row.get(0) instanceof Long ) {
				time = (long) row.get(0);
			} else {
				throw new IllegalStateException("Unsupported type: " + row.get(0).getClass());
			}
			BigDecimal value = new BigDecimal((String) row.get(1)), volume = new BigDecimal((String) row.get(2));
			result.add(new Item(category, symbol, time, value, volume));
		}
		return result;
	}
	
	public static List<Item> toItems(CatSym cs, List<List<Object>> rows) {
		return toItems(cs.category, cs.symbol, rows);
	}
	
	public static Item registerItem(Item item) {
		List<Item> list = databaseReplica.get(item.category);
		if ( list == null ) {
			list = new ArrayList<>();
			databaseReplica.put(item.category, list);
		}
		list.add(item);
		return item;
	}
	
	public static Item registerItem(String category, String symbol, long time, BigDecimal value, BigDecimal volume) {
		return registerItem(new Item(category, symbol, time, value, volume));
	}
	
	public static Item registerItem(String category, String symbol, long time, String value, String volume) {
		return registerItem(category, symbol, time, new BigDecimal(value), new BigDecimal(volume));
	}
	
	public static Item registerItem(CatSym cs, long time, String value, String volume) {
		return registerItem(cs.category, cs.symbol, time, value, volume);
	}
	
	public static Item registerItem(CatSym cs, long time) {
		return registerItem(cs.newItem(time));
	}
	
	public static  List<Item> registeredItems(String category, String symbol) {
		List<Item> result = new ArrayList<>(), category_items = databaseReplica.get(category);
		if ( category_items == null ) {
			return result;
		}
		for ( Item item : category_items ) {
			if ( symbol.equals(item.symbol) ) {
				result.add(item);
			}
		}
		assertNotEquals(0, result.size());
		return result;
	}
	
	public static List<Item> registeredItems(CatSym cs) {
		return registeredItems(cs.category, cs.symbol);
	}
	
	public static List<String> registeredCategories() {
		List<String> result = new ArrayList<>(databaseReplica.keySet());
		Collections.sort(result);
		assertNotEquals(0, result.size());
		return result;
	}
	
	public static List<String> registeredSymbols(String category) {
		List<String> result = new ArrayList<>();
		for ( Item item : databaseReplica.get(category) ) {
			result.add(item.symbol);
		}
		Collections.sort(result);
		assertNotEquals(0, result.size());
		return result;
	}
		
	protected final List<String> backnodeHosts = new ArrayList<>();
	
	public BacknodeIT() {
		this.backnodeHosts.add("localhost:9698");
	}
	
	/**
	 * Get request specification for specified backnode host.
	 * <p>
	 * @param host - host in form host:port
	 * @return request specification
	 */
	protected RequestSpecification getSpec(String host) {
		return new RequestSpecBuilder()
				.setContentType(ContentType.JSON)
				.setBaseUri("http://" + host + "/api/v1/")
				.addFilter(new ResponseLoggingFilter())
				.addFilter(new RequestLoggingFilter())
				.build();		
	}
	
	/**
	 * Get request specification for the first available backnode host.
	 * <p>
	 * @return request specification
	 */
	protected RequestSpecification getSpec() {
		return getSpec(backnodeHosts.get(0));
	}
	
	/**
	 * Randomly get request specification for one of available  backnode hosts.
	 * <p>
	 * @return request specification
	 */
	protected RequestSpecification getSpecRandom() {
		return getSpec(backnodeHosts.get(ThreadLocalRandom.current().nextInt(backnodeHosts.size())));
	}
	
	/**
	 * Get collection of request specifications for each existings backnode hosts.
	 * <p>
	 * @return list of available request specification
	 */
	protected Collection<RequestSpecification> getSpecAll() {
		List<RequestSpecification> result = new ArrayList<>();
		for ( String host : backnodeHosts ) {
			result.add(getSpec(host));
		}
		return result;
	}
		
	protected PingResponseDTO apiPing(RequestSpecification spec) {
		return given()
				.spec(spec)
			.when()
				.get("ping")
			.then()
				.statusCode(200)
				.extract()
				.as(PingResponseDTO.class);
	}
	
	protected ItemResponseDTO apiPutItem(RequestSpecification spec, Item item) {
		return given()
				.spec(spec)
				.contentType(ContentType.URLENC)
				.formParam("symbol", item.symbol)
				.formParam("time", item.time)
				.formParam("value", item.value)
				.formParam("volume", item.volume)
			.when()
				.put("item")
			.then()
				.statusCode(200)
				.extract()
				.as(ItemResponseDTO.class);
	}
	
	protected ItemResponseDTO apiPutItem(Item item) {
		return apiPutItem(getSpec(), item);
	}
	
	protected ItemResponseDTO apiPutItem(RequestSpecification spec, List<Item> items) {
		return given()
				.spec(spec)
				.contentType(ContentType.URLENC)
				.formParam("symbol", items.stream().map(x -> x.symbol).collect(Collectors.toList()))
				.formParam("time", items.stream().map(x -> x.time).collect(Collectors.toList()))
				.formParam("value", items.stream().map(x -> x.value).collect(Collectors.toList()))
				.formParam("volume", items.stream().map(x -> x.volume).collect(Collectors.toList()))
			.when()
				.put("item")
			.then()
				.statusCode(200)
				.extract()
				.as(ItemResponseDTO.class);
	}
	
	protected ItemsResponseDTO apiGetItems(RequestSpecification spec, String symbol, Integer limit) {
		spec = given()
				.spec(spec)
				.param("symbol", symbol);
		if ( limit != null ) spec = spec.param("limit", limit);
		return spec.when()
				.get("items")
			.then()
				.statusCode(200)
				.extract()
				.as(ItemsResponseDTO.class);		
	}
	
	protected ItemsResponseDTO apiGetItems(String symbol, Integer limit) {
		return apiGetItems(getSpec(), symbol, limit);
	}
	
	protected ItemsResponseDTO apiGetItems(String symbol) {
		return apiGetItems(getSpec(), symbol, null);
	}
	
	protected CategoriesResponseDTO apiGetCategories(RequestSpecification spec) {
		return given()
				.spec(spec)
			.when()
				.get("categories")
			.then()
				.statusCode(200)
				.extract()
				.as(CategoriesResponseDTO.class);		
	}
	
	protected CategoriesResponseDTO apiGetCategories() {
		return apiGetCategories(getSpec());
	}
	
	protected SymbolsResponseDTO apiGetSymbols(RequestSpecification spec,
			String category, String afterSymbol, Integer limit)
	{
		spec = given()
				.spec(spec)
				.param("category", category);
		if ( limit != null ) spec = spec.param("limit", limit);
		if ( afterSymbol != null ) spec = spec.param("afterSymbol", afterSymbol);
		return spec.when()
				.get("symbols")
			.then()
				.statusCode(200)
				.extract()
				.as(SymbolsResponseDTO.class);		
	}
	
	protected SymbolsResponseDTO apiGetSymbols(RequestSpecification spec, String category, Integer limit) {
		return apiGetSymbols(spec, category, null, limit);
	}
	
	protected SymbolsResponseDTO apiGetSymbols(RequestSpecification spec, String category, String afterSymbol) {
		return apiGetSymbols(spec, category, afterSymbol, null);
	}
	
	protected SymbolsResponseDTO apiGetSymbols(RequestSpecification spec, String category) {
		return apiGetSymbols(spec, category, null, null);
	}
	
	protected SymbolsResponseDTO apiGetSymbols(String category) {
		return apiGetSymbols(getSpec(), category);
	}
	
	protected SymbolUpdateResponseDTO apiPutSymbolUpdate(RequestSpecification spec,
			String symbol, long time, Map<Integer, String> tokens)
	{
		spec = given()
				.spec(spec)
				.param("symbol", symbol)
				.param("time", time);
		Iterator<Entry<Integer, String>> it = tokens.entrySet().iterator();
		while ( it.hasNext() ) {
			Entry<Integer, String> entry = it.next();
			spec.param(Integer.toString(entry.getKey()), entry.getValue());
		}
		return spec.when()
				.put("symbol/update")
			.then()
				.statusCode(200)
				.extract()
				.as(SymbolUpdateResponseDTO.class);
	}
	
	protected SymbolUpdateResponseDTO apiPutSymbolUpdate(RequestSpecification spec,
			String symbol, long time, Object... tokens)
	{
		return apiPutSymbolUpdate(spec, symbol, time, toMap(tokens));
	}
	
	protected void generateItems(String category, String symbol, int total_items,
			long start_time, long time_delta,
			BigDecimal start_value, BigDecimal value_delta,
			BigDecimal start_volume, BigDecimal volume_delta)
	{
		long time = start_time;
		BigDecimal value = start_value, volume = start_volume;
		for ( int i = 0; i < total_items; i ++ ) {
			assertNotError(apiPutItem(getSpecRandom(), registerItem(category, symbol, time, value, volume)));
			time += time_delta;
			value = value.add(value_delta);
			volume = volume.add(volume_delta);
		}
	}
	
	protected void generateItems(String category, String symbol, int total_items,
			long start_time, long time_delta,
			String start_value, String value_delta,
			String start_volume, String volume_delta)
	{
		generateItems(category, symbol, total_items, start_time, time_delta,
				new BigDecimal(start_value), new BigDecimal(value_delta),
				new BigDecimal(start_volume), new BigDecimal(volume_delta));
	}

	@Before
	public void setUp() {
		
	}
	
	@After
	public void tearDown() {
		databaseReplica.clear();
		existingSymbols.clear();
		given()
			.spec(getSpec())
		.when()
			.get("clear")
		.then()
			.statusCode(200);
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
	public void C1005_PutItem_ShouldRegisterSymbols() {
		long time = 10000L;
		for ( CatSym cs : newSymbols(5, 10) ) {
			apiPutItem(getSpecRandom(), registerItem(cs, time ++));
		}
		
		for ( RequestSpecification spec : getSpecAll() ) {
			for ( String category : registeredCategories() ) {
				SymbolsResponseDTO response = apiGetSymbols(spec, category);
				
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
	
	// TODO: refactor me 

	@Test
	public void C1050_Items_AllShouldBeLimitedUpTo5000() throws Exception {
		final String category = CAT4, symbol = CAT4_SYMBOL1;
		long time_delta = 30000L; // +30 seconds for each
		int total_m1_tuples = 4 * 24 * 60, total_items = (int) (total_m1_tuples * 60000 / time_delta);
		assertThat(total_items, is(greaterThan(5000 * 2)));
		generateItems(category, symbol, total_items, 10000L, time_delta, "0.001", "0.001", "5", "5");

		CompletableFuture<ItemsResponseDTO> x = new CompletableFuture<>();
		await().dontCatchUncaughtExceptions()
			.pollInterval(Duration.ofSeconds(10L))
			.atMost(Duration.ofSeconds(30)).until(() -> {
			ItemsResponseDTO response = apiGetItems(symbol);
			assertNotError(response);
			if ( response.data.rows.size() == 5000 ) {
				x.complete(response);
				return true;
			} else {
				return false;
			}
		});
		
		ItemsResponseDTO response = x.get(1, TimeUnit.SECONDS);
		assertEquals(symbol, response.data.symbol);
		assertEquals("std", response.data.format);
		assertEquals(5000, response.data.rows.size());
		// expected 5000 records but sequence is mixed with other symbols
		assertThat(response.data.fromOffset, is(greaterThanOrEqualTo(5000L)));
		assertNotNull(response.data.magic);
		assertThat(response.data.magic.length(), is(greaterThanOrEqualTo(1)));
		// don't check to allow run outside of whole sequence
		//assertEquals("4817b7c937aaf6e571620e6914b7f983", response.data.magic);
		List<Item> actual = toItems(category, symbol, response.data.rows);
		List<Item> expected = registeredItems(category, symbol).subList(0, 5000);
		assertEquals(expected, actual);
	}
	
	@Test
	public void C1051_Items_WithLimitLessThanMaxLimitAndLessThanItemsCount() {
		final String category = CAT4, symbol = CAT4_SYMBOL2;
		generateItems(category, symbol, 800, 129800L, 15000L, "0.050", "0.025", "1", "1");
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, symbol, 500);
			
			assertNotError(response);
			assertEquals(symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(500, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			List<Item> actual = toItems(category, symbol, response.data.rows);
			List<Item> expected = registeredItems(category, symbol).subList(0, 500);
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1052_Items_WithLimitLessThanMaxLimitButGreaterThanItemsCount() {
		final String category = CAT4, symbol = CAT4_SYMBOL2;
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, symbol, 1000);
			
			assertNotError(response);
			assertEquals(symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(800, response.data.rows.size()); // added by prev test
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			List<Item> actual = toItems(category, symbol, response.data.rows);
			List<Item> expected = registeredItems(category, symbol).subList(0, 800);
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1053_Items_WithLimitGreaterThanMaxLimitAndLessThanItemsCount() {
		final String category = CAT4, symbol = CAT4_SYMBOL2;
		// it was 800, max limit is 5000, so add 4500 more to make 5300 total
		List<Item> prev_registered_items = registeredItems(category, symbol);
		Item last_item = prev_registered_items.get(prev_registered_items.size() - 1);
		BigDecimal value_delta = new BigDecimal("0.025"), volume_delta = BigDecimal.ONE;
		generateItems(category, symbol, 4500, last_item.time + 15000L, 15000L,
				last_item.value.add(value_delta), value_delta,
				last_item.volume.add(volume_delta), volume_delta);
		
		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response = apiGetItems(spec, symbol, 5200);
			
			assertEquals(symbol, response.data.symbol);
			assertEquals("std", response.data.format);
			assertEquals(5000, response.data.rows.size());
			assertNotNull(response.data.magic);
			assertNotNull(response.data.fromOffset);
			List<Item> actual = toItems(category, symbol, response.data.rows);
			List<Item> expected = registeredItems(category, symbol).subList(0, 5000);
			assertEquals(expected, actual);
		}
	}
	
	@Test
	public void C1054_Items_WithLimitGreaterThanMaxLimitAndGreaterThanItemsCount() {
		fail();
	}
	
	@Test
	public void C1055_Items_FromTimeAndToTime() {
		fail();
	}
	
	@Test
	public void C1056_Items_FromOffset() {
		fail();
	}
	
	@Test
	public void C1057_Item_ContinueRequest() {
		fail();
	}
	
	@Test
	public void C1058_Items_ContinueRequest_OutOfRangeShouldBeOk() {
		fail();
	}
	
	@Test
	public void C1060_Items_ConcurrentRequestsShouldGiveSameResults() {
		fail();
	}

	@Test
	public void C1061_Items_TwoConsecutiveCallsShouldGiveSameResult() {
		fail();
	}
	
	@Test
	public void C1061_Items_AnyNodeShouldProvideDataOfAnySymbolDespitePartitioning() {
		fail();
	}
	
	// C2*** - Test Cases of features related to symbol and symbol updates
	
	@Test
	public void C2001_Symbols_WithLimit() {
		List<String> expected;
		SymbolsResponseDTO response;
		
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, "zoo", 2);
			assertNotError(response);
			expected = Arrays.asList("zoo@bar", "zoo@foo");
			assertEquals(expected, response.data.rows);
			assertEquals("zoo", response.data.category);
		}
	}
	
	@Test
	public void C2002_Symbols_WithAfterSymbol() {
		List<String> expected;
		SymbolsResponseDTO response;
		
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, "zoo", "zoo@foo");
			assertNotError(response);
			expected = Arrays.asList("zoo@gap", "zoo@lol");
			assertEquals(expected, response.data.rows);
			assertEquals("zoo", response.data.category);			
		}
	}
	
	@Test
	public void C2003_Symbols_WithLimitAndAfterSymbol() {
		List<String> expected;
		SymbolsResponseDTO response;
		
		for ( RequestSpecification spec : getSpecAll() ) {
			response = apiGetSymbols(spec, "zoo", "zoo@bar", 2);
			assertNotError(response);
			expected = Arrays.asList("zoo@foo", "zoo@gap");
			assertEquals(expected, response.data.rows);
			assertEquals("zoo", response.data.category);			
		}
	}
	
	@Test
	public void C2010_PutSymbol_PutAndTestResponse() {
		fail();
	}
	
	@Test
	public void C2011_PutSymbol_PutWithNoCategory() {
		fail();
	}
	
	@Test
	public void C2012_PutSymbol_ShouldRegisterCategory() {
		fail();
	}
	
	@Test
	public void C2030_Symbols_ManySymbolsWithLimitGreaterThanMaxLimit() {
		fail();
	}
	
	@Test
	public void C2031_Symbols_ManySymbolsWithLimitreaterThanMaxLimit_Continue() {
		fail();
	}
	
	@Test
	public void C2050_PutSymbolUpdate() {
		fail();
	}
	
	@Test
	public void C2051_SymbolUpdates() {
		fail();
	}
	
	@Test
	public void C2052_PutSymbolUpdate_NewSymbolOfExistingCategoryShouldBeRegistered() {
		fail();
	}
	
	@Test
	public void C2053_PutSymbolUpdate_NewSymbolOfNewCategoryBothShouldBeRegistered() {
		fail();
	}

	@Test
	public void C2054_PutSymbolUpdate_SameUpdateShouldOverrideExisting() {
		fail();
	}
	
	// C3*** - Test Cases of features related to categories
	
	@Test
	public void C3000_Categories_WithEmptyCategory() {
		fail();
	}
	
	// C9*** - Test Cases of features related to tuples
	
	@Test
	public void C9001_Tuples_All() {
		fail();
	}
	
	@Test
	public void C9002_Tuples_FromTimeToTimeAndLimit() {
		fail();
	}
	
	@Test
	public void C9003_Tuples_AllM1() {
		fail();
	}
	
	@Test
	public void C9004_Tuples_AllM2() {
		fail();
	}
	
	@Test
	public void C9005_Tuples_AllM3() {
		fail();
	}
	
	@Test
	public void C9006_Tuples_AllM5() {
		fail();
	}
	
	@Test
	public void C9007_Tuples_AllM6() {
		fail();
	}
	
	@Test
	public void C9008_Tuples_AllM10() {
		fail();
	}
	
	@Test
	public void C9009_Tuples_AllM12() {
		fail();
	}
	
	@Test
	public void C9010_Tuples_AllM15() {
		fail();
	}
	
	@Test
	public void C9011_Tuples_AllM20() {
		fail();
	}
	
	@Test
	public void C9012_Tuples_AllM30() {
		fail();
	}
	
	@Test
	public void C9013_Tuples_AllH1() {
		fail();
	}
	
	@Test
	public void C9014_Tuples_AllH2() {
		fail();
	}
	
	@Test
	public void C9015_Tuples_AllH3() {
		fail();
	}
	
	@Test
	public void C9016_Tuples_AllH4() {
		fail();
	}
	
	@Test
	public void C9017_Tuples_AllH6() {
		fail();
	}
	
	@Test
	public void C9018_Tuples_AllH8() {
		fail();
	}
	
	@Test
	public void C9019_Tuples_AllH12() {
		fail();
	}
	
	@Test
	public void C9020_Tuples_AllD1() {
		fail();
	}

}
