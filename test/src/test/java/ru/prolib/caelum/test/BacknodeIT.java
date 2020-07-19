package ru.prolib.caelum.test;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.*;
import static io.restassured.matcher.RestAssuredMatchers.*;
import static org.hamcrest.Matchers.*;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
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
	public static final String CAT1 = "zoo", CAT2 = "old", CAT3 = "bar";
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
	
	protected static final Map<String, List<Item>> databaseReplica = new HashMap<>();
	
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
	
	protected final List<String> backnodeHosts = new ArrayList<>();
	
	public BacknodeIT() {
		this.backnodeHosts.add("localhost:9698");
	}
	
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
	
	protected Item registerItem(Item item) {
		List<Item> list = databaseReplica.get(item.category);
		if ( list == null ) {
			list = new ArrayList<>();
			databaseReplica.put(item.category, list);
		}
		list.add(item);
		return item;
	}
	
	protected Item registerItem(String category, String symbol, long time, BigDecimal value, BigDecimal volume) {
		return registerItem(new Item(category, symbol, time, value, volume));
	}
	
	protected Item registerItem(String category, String symbol, long time, String value, String volume) {
		return registerItem(category, symbol, time, new BigDecimal(value), new BigDecimal(volume));
	}
	
	protected ItemResponseDTO apiPutItem(RequestSpecification spec, Item item) {
		return given()
				.spec(spec)
				.param("symbol", item.symbol)
				.param("time", item.time)
				.param("value", item.value)
				.param("volume", item.volume)
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
	
	protected ItemsResponseDTO apiGetItems(String symbol) {
		return given()
			.spec(getSpec())
			.param("symbol", symbol)
		.when()
			.get("items")
		.then()
			.statusCode(200)
			.extract()
			.as(ItemsResponseDTO.class);
	}
	
	protected CategoriesResponseDTO apiGetCategories() {
		return given()
			.spec(getSpec())
		.when()
			.get("categories")
		.then()
			.statusCode(200)
			.extract()
			.as(CategoriesResponseDTO.class);
	}
	
	@Before
	public void setUp() {
		
	}
	
	@Test
	public void C0001_Item_PutAndTestResponse() {
		Item item1 = registerItem(CAT1, CAT1_SYMBOL1, 10000L, "100.0000", "50000.00");
		
		ItemResponseDTO response = apiPutItem(item1);
		
		assertNotError(response);
	}
	
	@Test
	public void C0002_Item_PutOfSameTimeAndTestSequence() {
		assertNotError(apiPutItem(registerItem(CAT1, CAT1_SYMBOL1, 10000L, "100.0250", "125.20")));
		assertNotError(apiPutItem(registerItem(CAT1, CAT1_SYMBOL1, 10000L, "100.0391", "575.16")));
		
		ItemsResponseDTO response = apiGetItems(CAT1_SYMBOL1);
	
		assertNotError(response);
		assertEquals(CAT1_SYMBOL1, response.data.symbol);
		assertEquals("std", response.data.format);
		assertEquals(3, (long) response.data.fromOffset);
		assertNotNull(response.data.magic);
		assertThat(response.data.magic.length(), is(greaterThanOrEqualTo(1)));
		assertEquals("2c54183c19c2c37c298ee8f1906ad1d3", response.data.magic);
		assertEquals(3, (long) response.data.fromOffset);
		List<Item> actual = toItems(CAT1, CAT1_SYMBOL1, response.data.rows);
		List<Item> expected = databaseReplica.get(CAT1);
		assertEquals(expected, actual);
	}
	
	@Test
	public void C0003_Item_OfDifferentSymbols() {
		assertNotError(apiPutItem(registerItem(CAT1, CAT1_SYMBOL2, 10100L,    "0.1504",  "296.0970")));
		assertNotError(apiPutItem(registerItem(CAT1, CAT1_SYMBOL3, 10100L,   "12",        "96")));
		assertNotError(apiPutItem(registerItem(CAT1, CAT1_SYMBOL4, 10100L,   "84.19",    "100")));
		assertNotError(apiPutItem(registerItem(CAT2, CAT2_SYMBOL1, 12000L,  "420.096",  "9000")));
		assertNotError(apiPutItem(registerItem(CAT2, CAT2_SYMBOL2, 12000L,  "180.015",  "1000")));
		assertNotError(apiPutItem(registerItem(CAT2, CAT2_SYMBOL3, 12000L, "1216",      "5500")));
		assertNotError(apiPutItem(registerItem(CAT3, CAT3_SYMBOL1, 10000L,  "553.219",    "85")));
		assertNotError(apiPutItem(registerItem(CAT3, CAT3_SYMBOL2, 10000L,   "26.005",    "10")));
		assertNotError(apiPutItem(registerItem(CAT3, CAT3_SYMBOL3, 10000L,   "94.108",     "1")));		
	}
	
	@Test
	public void C0004_Categories() {
		CategoriesResponseDTO response = apiGetCategories();
		
		assertNotError(response);
		List<String> expected = Arrays.asList(CAT3, CAT2, CAT1);
		assertEquals(expected, response.data.rows);
	}

}
