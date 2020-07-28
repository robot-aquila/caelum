package ru.prolib.caelum.test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.test.dto.CategoriesResponseDTO;
import ru.prolib.caelum.test.dto.ItemResponseDTO;
import ru.prolib.caelum.test.dto.ItemsResponseDTO;
import ru.prolib.caelum.test.dto.PingResponseDTO;
import ru.prolib.caelum.test.dto.ResponseDTO;
import ru.prolib.caelum.test.dto.SymbolResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdateResponseDTO;
import ru.prolib.caelum.test.dto.SymbolUpdatesResponseDTO;
import ru.prolib.caelum.test.dto.SymbolsResponseDTO;
import ru.prolib.caelum.test.dto.TuplesResponseDTO;

public class TestBasis {

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
	
	public static class Tuple {
		final long time;
		final BigDecimal open, high, low, close, volume;
		
		public Tuple(long time, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, BigDecimal volume) {
			this.time = time;
			this.open = open;
			this.high = high;
			this.low = low;
			this.close = close;
			this.volume = volume;
		}
		
		public Tuple(long time, String open, String high, String low, String close, String volume) {
			this(time, new BigDecimal(open), new BigDecimal(high), new BigDecimal(low),
					new BigDecimal(close), new BigDecimal(volume));
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("time", time)
					.append("open", open)
					.append("high", high)
					.append("low", low)
					.append("close", close)
					.append("volume", volume)
					.build();
		}
		
		@Override
		public int hashCode() {
			return new HashCodeBuilder()
					.append(time)
					.append(open)
					.append(high)
					.append(low)
					.append(close)
					.append(volume)
					.build();
		}
		
		@Override
		public boolean equals(Object other) {
			if ( other == this ) {
				return true;
			}
			if ( other == null || other.getClass() != Tuple.class ) {
				return false;
			}
			Tuple o = (Tuple) other;
			return new EqualsBuilder()
					.append(o.time, time)
					.append(o.open, open)
					.append(o.high, high)
					.append(o.low, low)
					.append(o.close, close)
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
	
	private static final Map<String, List<Item>> databaseReplica = new HashMap<>();
	private static final Map<String, Set<String>> existingCategories = new HashMap<>();
	private static final Set<String> existingSymbols = new HashSet<>();

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

	public static int getSymbolPartition(String symbol, int num_partitions) {
		byte[] key_bytes = Serdes.String().serializer().serialize(null, symbol);
		int partition = Utils.toPositive(Utils.murmur2(key_bytes)) % num_partitions;
		return partition;
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

	public static Map<Integer, List<CatSym>> newSymbolsOfDifferentPartitions(int num_partitions_max) {
		int num_partitions_min = 2;
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
		return map;
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
	
	public static void assertNotError(SymbolUpdateResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNull(response.data);
	}
	
	public static void assertNotError(SymbolUpdatesResponseDTO response) {
		assertNotError((ResponseDTO) response);
		assertNotNull(response.data);
	}
	
	public static void assertNotError(TuplesResponseDTO response) {
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
	
	public static List<Tuple> toTuples(List<List<Object>> rows) {
		List<Tuple> result = new ArrayList<>();
		for ( List<Object> row : rows ) {
			long time = 0;
			Object raw_time = row.get(0);
			if ( raw_time instanceof Integer ) {
				time = (int) raw_time;
			} else if ( raw_time instanceof Long ) {
				time = (long) raw_time;
			} else {
				throw new IllegalStateException("Unsupported type: " + raw_time);
			}
			result.add(new Tuple(time,
					new BigDecimal((String) row.get(1)),
					new BigDecimal((String) row.get(2)),
					new BigDecimal((String) row.get(3)),
					new BigDecimal((String) row.get(4)),
					new BigDecimal((String) row.get(5))));
		}
		return result;
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
	
	protected final List<String> backnodeHosts;

	public TestBasis(Collection<String> backnode_hosts) {
		backnodeHosts = new ArrayList<>(backnode_hosts);
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
	
	
	protected TuplesResponseDTO apiGetTuples(RequestSpecification spec, Period period, String symbol,
			Integer limit, Long from, Long to)
	{
		spec = given()
				.spec(spec)
				.pathParam("period", period)
				.param("symbol", symbol);
		if ( limit != null ) spec = spec.param("limit", limit);
		if ( from != null ) spec = spec.param("from", from);
		if ( to != null ) spec = spec.param("to", to);
		return spec.when()
				.get("tuples/{period}")
			.then()
				.statusCode(200)
				.extract()
				.as(TuplesResponseDTO.class);
	}
	
	protected TuplesResponseDTO apiGetTuples(RequestSpecification spec, Period period, String symbol) {
		return apiGetTuples(spec, period, symbol, null, null, null);
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
				.contentType(ContentType.URLENC)
				.formParam("symbol", symbol)
				.formParam("time", time);
		Iterator<Entry<Integer, String>> it = tokens.entrySet().iterator();
		while ( it.hasNext() ) {
			Entry<Integer, String> entry = it.next();
			spec.formParam(Integer.toString(entry.getKey()), entry.getValue());
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
	
	protected SymbolUpdatesResponseDTO apiGetSymbolUpdates(RequestSpecification spec, String symbol) {
		return given()
				.spec(spec)
				.param("symbol", symbol)
			.when()
				.get("symbol/updates")
			.then()
				.statusCode(200)
				.extract()
				.as(SymbolUpdatesResponseDTO.class);		
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
	
	public void setUp() {
		
	}
	
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

}
