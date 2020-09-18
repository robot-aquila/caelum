package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.EventKey;

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
	
	@SuppressWarnings("unlikely-arg-type")
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
	public void testGetSpaceEvents1() {
		Subspace actual = service.getSpaceEvents("bamba");

		Subspace expected = new Subspace(Tuple.from("test", 0x03, "bamba"));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyEvent3() {
		byte[] actual = service.getKeyEvent("zulu-15", 15882668192L, 1001);
		
		byte[] expected = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L, 1001)).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyEvent1() {
		byte[] actual = service.getKeyEvent(new EventKey("zulu-15", 15882668192L, 2480));
		
		byte[] expected = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L, 2480)).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testGetKeyEvent2() {
		byte[] actual = service.getKeyEvent("bobby", 1222348899L);
		
		byte[] expected = new Subspace(Tuple.from("test", 0x03, "bobby", 1222348899L)).pack();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testParseKeyEvent() {
		byte[] source = new Subspace(Tuple.from("test", 0x03, "zulu-15", 15882668192L, 85026)).pack();
		
		EventKey actual = service.parseKeyEvent(source);
		
		EventKey expected = new EventKey("zulu-15", 15882668192L, 85026);
		assertEquals(expected, actual);
	}

	@Test
	public void testPackEventData() {
		byte actual[] = service.packEventData("lumbaza capuccino");
		
		byte expected[] = "lumbaza capuccino".getBytes();
		assertArrayEquals(expected, actual);
	}
	
	@Test
	public void testUnpackEventData() {
		String actual = service.unpackEventData("kabambaber".getBytes());
		
		assertEquals("kabambaber", actual);
	}
	
	@Test
	public void testUnpackEvent() {
		KeyValue source = new KeyValue(new Subspace(Tuple.from("test", 0x03, "zyamba", 15778003120L, 5001)).pack(),
				"tulusa visconci".getBytes());
		
		Pair<EventKey, String> actual = service.unpackEvent(source);
		
		Pair<EventKey, String> expected = Pair.of(new EventKey("zyamba", 15778003120L, 5001), "tulusa visconci");
		assertEquals(expected, actual);
	}

}
