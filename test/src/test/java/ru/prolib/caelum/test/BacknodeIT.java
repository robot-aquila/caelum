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
import java.util.concurrent.CountDownLatch;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
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
	
	public int getSymbolPartition(String symbol, int num_partitions) {
		byte[] key_bytes = Serdes.String().serializer().serialize(null, symbol);
		int partition = Utils.toPositive(Utils.murmur2(key_bytes)) % num_partitions;
		return partition;
	}
	
	public static class InitialAndDelta {
		final BigDecimal initial, delta;
		
		public InitialAndDelta(BigDecimal initial, BigDecimal delta) {
			this.initial = initial;
			this.delta = delta;
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("initial", initial)
					.append("delta", delta)
					.build();
		}
		
	}
	
	public static BigDecimal toBD(long value, int scale) {
		return new BigDecimal(value).divide(BigDecimal.TEN.pow(scale));
	}
	
	public static InitialAndDelta randomInitialAndDelta() {
		ThreadLocalRandom r = ThreadLocalRandom.current();
		int[] tick_tmp = { 1, 2, 5, };
		BigDecimal tick = new BigDecimal(tick_tmp[r.nextInt(tick_tmp.length)]);
		int scale = r.nextInt(0, 16);
		tick = tick.pow(r.nextInt(1, 11)).divide(BigDecimal.TEN.pow(scale));
		return new InitialAndDelta(tick.multiply(new BigDecimal(r.nextLong(1L, 2000L))), tick);
	}

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
		
		public Item newItem(long time, BigDecimal value, BigDecimal volume) {
			return new Item(category, symbol, time, value, volume);
		}
		
		public Item newItem(long time, String value, String volume) {
			return newItem(time, new BigDecimal(value), new BigDecimal(volume));
		}
		
		/**
		 * Create new item of this symbol with random value and volume.
		 * <p>
		 * @param time - item time
		 * @return new item instance
		 */
		public Item newItem(long time) {
			return newItem(time, toBD(r.nextLong(), decimals), toBD(r.nextLong(), volumeDecimals));
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("s", symbol)
					.append("d", decimals)
					.append("vd", volumeDecimals)
					.build();
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
	protected static final Map<String, Set<String>> existingCategories = new HashMap<>();
	protected static final Set<String> existingSymbols = new HashSet<>();
	
	public static CatSym registerSymbol(CatSym cs) {
		existingSymbols.add(cs.symbol);
		Set<String> symbols = existingCategories.get(cs.category);
		if ( symbols == null ) {
			symbols = new HashSet<>();
			existingCategories.put(cs.category, symbols);
		}
		symbols.add(cs.symbol);
		return cs;
	}
	
	public static CatSym newSymbol(String category) {
		for ( int i = 0; i < 1000; i ++ ) {
			CatSym cs = CatSym.random(category);
			if ( ! existingSymbols.contains(cs.symbol) ) {
				return registerSymbol(cs);
			}
		}
		throw new IllegalStateException();		
	}
	
	public static CatSym newSymbol() {
		for ( int i = 0; i < 1000; i ++ ) {
			CatSym cs = CatSym.random();
			if ( ! existingSymbols.contains(cs.symbol) ) {
				return registerSymbol(cs);
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
		assertThat(timeDiff(response), is(lessThanOrEqualTo(5000L)));
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
	
	public static void assertNotError(SymbolResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNull(response.data);
	}
	
	public static void assertNotError(SymbolsResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNotNull(response.data);
	}
	
	public static void assertNotError(CategoriesResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNotNull(response.data);
	}
	
	public static void assertEqualsItemByItem(String msg, List<Item> expected, List<Item> actual) {
		int count = Math.min(expected.size(), actual.size());
		for ( int i = 0; i < count; i ++ ) {
			assertEquals(msg + ": item mismatch #" + i, expected.get(i), actual.get(i));
		}
		assertEquals(msg + ": list size mismatch", expected.size(), actual.size());
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
			registerSymbol(new CatSym(item.category, item.symbol, (byte)item.value.scale(), (byte)item.volume.scale()));
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
		List<String> result = new ArrayList<>(existingCategories.keySet());
		Collections.sort(result);
		assertNotEquals(0, result.size());
		return result;
	}
	
	public static List<String> registeredSymbols(String category) {
		Set<String> dummy_set = existingCategories.get(category);
		if ( dummy_set == null ) dummy_set = new HashSet<>();
		List<String> result = new ArrayList<>(dummy_set);
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
	
	protected SymbolResponseDTO apiPutSymbol(RequestSpecification spec, String symbol) {
		return given()
				.spec(spec)
				.contentType(ContentType.URLENC)
				.formParam("symbol", symbol)
			.when()
				.put("symbol")
			.then()
				.statusCode(200)
				.extract()
				.as(SymbolResponseDTO.class);
	}
	
	protected SymbolResponseDTO apiPutSymbol(RequestSpecification spec, List<String> symbols) {
		return given()
				.spec(spec)
				.contentType(ContentType.URLENC)
				.formParam("symbol", symbols)
			.when()
				.put("symbol")
			.then()
			.statusCode(200)
			.extract()
			.as(SymbolResponseDTO.class);
	}
	
	protected SymbolResponseDTO apiPutSymbol(RequestSpecification spec, CatSym cs) {
		return apiPutSymbol(spec, cs.symbol);
	}
	
	protected SymbolResponseDTO apiPutSymbolCS(RequestSpecification spec, List<CatSym> symbols) {
		return apiPutSymbol(spec, symbols.stream().map(x -> x.symbol).collect(Collectors.toList()));
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
	
	protected ItemsResponseDTO apiGetItems(RequestSpecification spec, String symbol,
			Integer limit, Long from, Long to, String magic, Long fromOffset)
	{
		spec = given()
				.spec(spec)
				.param("symbol", symbol);
		if ( limit != null ) spec = spec.param("limit", limit);
		if ( from != null ) spec = spec.param("from", from);
		if ( to != null ) spec = spec.param("to", to);
		if ( magic != null ) spec = spec.param("magic", magic);
		if ( fromOffset != null ) spec = spec.param("fromOffset", fromOffset);
		return spec.when()
				.get("items")
			.then()
				.statusCode(200)
				.extract()
				.as(ItemsResponseDTO.class);
	}
	
	protected ItemsResponseDTO apiGetItems(RequestSpecification spec,
			String symbol, Integer limit, Long from, Long to)
	{
		return apiGetItems(spec, symbol, limit, from, to, null, null);
	}
	
	protected ItemsResponseDTO apiGetItems(RequestSpecification spec, String symbol, Integer limit) {
		return apiGetItems(spec, symbol, limit, null, null);
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
		List<Item> items = new ArrayList<>();
		int batch_size = 500;
		for ( int i = 0; i < total_items; i ++ ) {
			items.add(registerItem(category, symbol, time, value, volume));
			if ( items.size() >= batch_size ) {
				assertNotError(apiPutItem(getSpecRandom(), items));
				items.clear();
			}
			time += time_delta;
			value = value.add(value_delta);
			volume = volume.add(volume_delta);
		}
		if ( items.size() > 0 ) {
			assertNotError(apiPutItem(getSpecRandom(), items));
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
	
	protected void generateItems(CatSym cs, int total_items, long start_time, long time_delta,
			String start_value, String value_delta, String start_volume, String volume_delta)
	{
		generateItems(cs.category, cs.symbol, total_items, start_time, time_delta,
				start_value, value_delta, start_volume, volume_delta);
	}
	
	protected void generateItems(CatSym cs, int total_items) {
		ThreadLocalRandom r = ThreadLocalRandom.current();
		long start_time = r.nextLong(1000L, 10000000L);
		long time_delta = r.nextLong(1000L, 300000L);
		InitialAndDelta id_value = randomInitialAndDelta(), id_volume = randomInitialAndDelta();
		generateItems(cs.category, cs.symbol, total_items, start_time, time_delta,
				id_value.initial, id_value.delta, id_volume.initial, id_volume.delta);
	}

	@Before
	public void setUp() {
		
	}
	
	@After
	public void tearDown() {
		databaseReplica.clear();
		existingSymbols.clear();
		existingCategories.clear();
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
	public void C1063_Items_TwoConsecutiveCallsShouldGiveSameResult() {
		CatSym cs = newSymbol();
		generateItems(cs, 4000);

		for ( RequestSpecification spec : getSpecAll() ) {
			ItemsResponseDTO response1, response2;
			assertNotError(response1 = apiGetItems(spec, cs.symbol, 3000));
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
		// Prepare symbols
		int num_partitions_min = 2, num_partitions_max = 5;
		Map<Integer, List<CatSym>> map = new LinkedHashMap<>();
		Set<String> all_symbols = new HashSet<>();
		for ( int num_partitions = num_partitions_min; num_partitions <= num_partitions_max; num_partitions ++ ) {
			// We need one symbol per partition. Put symbols to list where symbol position is its partition.
			List<CatSym> symbols = new ArrayList<>();
			for ( int partition = 0; partition < num_partitions; partition ++ ) {
				for ( int k = 0; k < 5000; k ++ ) {
					CatSym cs = newSymbol();
					if ( all_symbols.contains(cs.symbol) == false
					  && getSymbolPartition(cs.symbol, num_partitions) == partition )
					{
						all_symbols.add(cs.symbol);
						symbols.add(cs);
						break;
					}
				}
			}
			assertEquals(num_partitions, symbols.size());
			map.put(num_partitions, symbols);
		}
		// Prepare fixture
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
	
	@Ignore
	@Test
	public void C2050_PutSymbolUpdate() {
		fail();
	}
	
	@Ignore
	@Test
	public void C2051_PutSymbolUpdate_NewSymbolOfExistingCategoryShouldBeRegistered() {
		fail();
	}
	
	@Ignore
	@Test
	public void C2052_PutSymbolUpdate_NewSymbolOfNewCategoryBothShouldBeRegistered() {
		fail();
	}

	@Ignore
	@Test
	public void C2053_PutSymbolUpdate_SameUpdateShouldOverrideExisting() {
		fail();
	}	
	
	@Ignore
	@Test
	public void C2060_SymbolUpdates() {
		fail();
	}
	

	
	// C3*** - Test Cases of features related to categories
	
	@Ignore
	@Test
	public void C3000_Categories_WithEmptyCategory() {
		fail();
	}
	
	// C9*** - Test Cases of features related to tuples
	
	@Ignore
	@Test
	public void C9001_Tuples_All() {
		
//		CompletableFuture<ItemsResponseDTO> x = new CompletableFuture<>();
//		await().dontCatchUncaughtExceptions()
//			.pollInterval(Duration.ofSeconds(10L))
//			.atMost(Duration.ofSeconds(30)).until(() -> {
//			ItemsResponseDTO response = apiGetItems(symbol);
//			assertNotError(response);
//			if ( response.data.rows.size() == 5000 ) {
//				x.complete(response);
//				return true;
//			} else {
//				return false;
//			}
//		});

		
		fail();
	}
	
	@Ignore
	@Test
	public void C9002_Tuples_FromTimeToTimeAndLimit() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9003_Tuples_AllM1() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9004_Tuples_AllM2() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9005_Tuples_AllM3() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9006_Tuples_AllM5() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9007_Tuples_AllM6() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9008_Tuples_AllM10() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9009_Tuples_AllM12() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9010_Tuples_AllM15() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9011_Tuples_AllM20() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9012_Tuples_AllM30() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9013_Tuples_AllH1() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9014_Tuples_AllH2() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9015_Tuples_AllH3() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9016_Tuples_AllH4() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9017_Tuples_AllH6() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9018_Tuples_AllH8() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9019_Tuples_AllH12() {
		fail();
	}
	
	@Ignore
	@Test
	public void C9020_Tuples_AllD1() {
		fail();
	}

}
