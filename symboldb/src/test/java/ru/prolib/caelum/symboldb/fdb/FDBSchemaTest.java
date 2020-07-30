package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.SymbolTime;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSchemaTest {
	FDBSchema service;

	@Before
	public void setUp() throws Exception {
		service = new FDBSchema(new Subspace(Tuple.from("test")));
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(7001927, 75)
				.append(new Subspace(Tuple.from("test")))
				.build();

		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBSchema(new Subspace(Tuple.from("test")))));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBSchema(new Subspace(Tuple.from("goha")))));
	}
	
	@Test
	public void testGetTrueBytes() {
		byte expected[] = Tuple.from(true).pack();
		
		assertArrayEquals(expected, service.getTrueBytes());
	}
	
	@Test
	public void testGetSpace() {
		Subspace actual = service.getSpace();
		
		Subspace expected = new Subspace(Tuple.from("test"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testPackTokens() {
		Map<Integer, String> source = new LinkedHashMap<>();
		source.put(1001, "zulu24");
		source.put(1002, "hello, world!");
		source.put(5008, "world of magic");
		
		byte actual[] = service.packTokens(source);
		
		byte expected[] = new Tuple()
				.add(1001).add("zulu24")
				.add(1002).add("hello, world!")
				.add(5008).add("world of magic")
				.pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testPackTokens_EmptyMap() {
		byte actual[] = service.packTokens(new LinkedHashMap<>());
		
		byte expected[] = new byte[0];
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testUnpackTokens() {
		byte source[] = new Tuple()
				.add(1001).add("zulu24")
				.add(1002).add("hello, world!")
				.add(5008).add("world of magic")
				.pack();
		
		Map<Integer, String> actual = service.unpackTokens(source);
		
		Map<Integer, String> expected = new HashMap<>();
		expected.put(1001, "zulu24");
		expected.put(1002, "hello, world!");
		expected.put(5008, "world of magic");
		assertEquals(expected, actual);
	}
	
	@Test
	public void testUnpackTokens_EmptyMap() {
		
		Map<Integer, String> actual = service.unpackTokens(new byte[0]);

		Map<Integer, String> expected = new HashMap<>();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testUnpackTokens_ThrowsIfUnevenSize() {
		byte source[] = new Tuple()
				.add(1001).add("zulu24")
				.add(1002)
				.pack();
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.unpackTokens(source));
		assertEquals("Uneven amount of elements", e.getMessage());
	}
	
	@Test
	public void testGetSpaceCategory() {
		Subspace actual = service.getSpaceCategory();
		
		Subspace expected = new Subspace(Tuple.from("test", 0x01));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyCategory() {
		byte actual[] = service.getKeyCategory("lumumba");
		
		byte expected[] = new Subspace(Tuple.from("test", 0x01, "lumumba")).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testParseKeyCategory() {
		byte source[] = new Subspace(Tuple.from("test", 0x01, "lumumba")).pack();
		
		String actual = service.parseKeyCategory(source);
		
		String expected = "lumumba";
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetSpaceCategorySymbol0() {
		Subspace actual = service.getSpaceCategorySymbol();
		
		Subspace expected = new Subspace(Tuple.from("test", 0x02));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetSpaceCategorySymbol1() {
		Subspace actual = service.getSpaceCategorySymbol("gamma");
		
		Subspace expected = new Subspace(Tuple.from("test", 0x02, "gamma"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyCategorySymbol() {
		byte actual[] = service.getKeyCategorySymbol("zumba", "zumba@bumba");
		
		byte expected[] = new Subspace(Tuple.from("test", 0x02, "zumba", "zumba@bumba")).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyCategorySymbol1() {
		byte actual[] = service.getKeyCategorySymbol(new CategorySymbol("bubba", "kappa"));
		
		byte expected[] = new Subspace(Tuple.from("test", 0x02, "bubba", "kappa")).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testParseKeyCategorySymbol() {
		byte source[] = new Subspace(Tuple.from("test", 0x02, "bubba", "zeta")).pack();
		
		CategorySymbol actual = service.parseKeyCategorySymbol(source);
		
		CategorySymbol expected = new CategorySymbol("bubba", "zeta");
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetSpaceSymbolUpdate1() {
		Subspace actual = service.getSpaceSymbolUpdate("bamba");

		Subspace expected = new Subspace(Tuple.from("test", 0x03, "bamba"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetKeySymbolUpdate2() {
		byte[] actual = service.getKeySymbolUpdate("zulu-15", 15882668192L);
		
		byte[] expected = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L)).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testGetKeySymbolUpdate1() {
		byte[] actual = service.getKeySymbolUpdate(new SymbolTime("zulu-15", 15882668192L));
		
		byte[] expected = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L)).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testParseKeySymbolUpdate() {
		byte[] source = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L)).pack();
		
		SymbolTime actual = service.parseKeySymbolUpdate(source);
		
		SymbolTime expected = new SymbolTime("zulu-15", 15882668192L);
		assertEquals(expected, actual);
	}

	@Test
	public void testPackSymbolUpdate() {
		Map<Integer, String> tokens = new LinkedHashMap<>();
		tokens.put(1200, "foo");
		tokens.put(1201, "bar");
		tokens.put(1300, "buz");
		SymbolUpdate source = new SymbolUpdate("symbol", 16627822990L, tokens);
		
		KeyValue actual = service.packSymbolUpdate(source);
		
		KeyValue expected = new KeyValue(
			new Subspace(Tuple.from("test", 0x03, "symbol", 16627822990L)).pack(),
			Tuple.from(1200, "foo", 1201, "bar", 1300, "buz").pack());
		assertEquals(expected, actual);
	}
	
	@Test
	public void testUnpackSymbolUpdate() {
		KeyValue source = new KeyValue(
				new Subspace(Tuple.from("test", 0x03, "zyamba", 15778003120L)).pack(),
				Tuple.from(1345, "alpha", 1346, "beta", 1347, "gamma").pack());
		
		SymbolUpdate actual = service.unpackSymbolUpdate(source);
		
		Map<Integer, String> expected_tokens = new LinkedHashMap<>();
		expected_tokens.put(1345, "alpha");
		expected_tokens.put(1346, "beta");
		expected_tokens.put(1347, "gamma");
		SymbolUpdate expected = new SymbolUpdate("zyamba", 15778003120L, expected_tokens);
		assertEquals(expected, actual);
	}

}
