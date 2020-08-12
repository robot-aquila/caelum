package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class SymbolUpdateTest {
	LinkedHashMap<Integer, String> tokens;
	SymbolUpdate service;

	@Before
	public void setUp() throws Exception {
		tokens = new LinkedHashMap<>();
		tokens.put(101, "foo");
		tokens.put(102, "bar");
		tokens.put(103, "buz");
		service = new SymbolUpdate("zulu", 15728298901L, tokens);
	}
	
	@Test
	public void testCtor() {
		assertEquals("zulu", service.getSymbol());
		assertEquals(15728298901L, service.getTime());
		assertEquals(tokens, service.getTokens());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("SymbolUpdate[symbol=zulu,time=15728298901,tokens={")
				.append("101=foo, 102=bar, 103=buz")
				.append("}]")
				.toString();
		
		assertEquals(expected, service.toString());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(107789, 405)
				.append("zulu")
				.append(15728298901L)
				.append(tokens)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		LinkedHashMap<Integer, String> tokens1 = new LinkedHashMap<>(), tokens2 = new LinkedHashMap<>();
		tokens1.put(101, "foo");
		tokens1.put(102, "bar");
		tokens1.put(103, "buz");
		tokens2.put(18210, "zumba");
		tokens2.put(19210, "bumba");
		tokens2.put(14850, "kappa");
		
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolUpdate("zulu", 15728298901L, tokens1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new SymbolUpdate("aaaa", 15728298901L, tokens1)));
		assertFalse(service.equals(new SymbolUpdate("zulu", 11111111111L, tokens1)));
		assertFalse(service.equals(new SymbolUpdate("zulu", 15728298901L, tokens2)));
		assertFalse(service.equals(new SymbolUpdate("aaaa", 11111111111L, tokens2)));
	}

}
