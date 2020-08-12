package ru.prolib.caelum.utils;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.utils.TradeGenerator.SymbolDesc;

public class TradeGeneratorTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testRecalcVolatility() {
		double delta = 0.000001d;
		SymbolDesc desc = new SymbolDesc("XXX", (byte)2, (byte)0, 10000);
		assertEquals(0.15d, desc.getVolatility(), delta);
		
		assertEquals(0.15d, desc.recalcVolatility(0.25d), delta);
		assertEquals(0.034166d, desc.getVolatility(), delta);
		
		assertEquals(0.034166d, desc.recalcVolatility(-0.25d), delta);
		assertEquals(0.034166d, desc.getVolatility(), delta);
		
		assertEquals(0.034166d, desc.recalcVolatility(-0.86d), delta);
		assertEquals(0.093133d, desc.getVolatility(), delta);
		
		assertEquals(0.093133d, desc.recalcVolatility(0.86d), delta);
		assertEquals(0.093133d, desc.getVolatility(), delta);
		
		assertEquals(0.093133d, desc.recalcVolatility(0.99d), delta);
		assertEquals(0.105700d, desc.getVolatility(), delta);
		
		assertEquals(0.105700d, desc.recalcVolatility(1.0d), delta);
		assertEquals(0.106666d, desc.getVolatility(), delta);
		
		assertEquals(0.106666d, desc.recalcVolatility(-1.0), delta);
		assertEquals(0.106666d, desc.getVolatility(), delta);
		
		assertEquals(0.106666d, desc.recalcVolatility(2.99d), delta);
		assertEquals(0.299033d, desc.getVolatility(), delta);
		
		assertEquals(0.299033d, desc.recalcVolatility(-26.0d), delta);
		assertEquals(0.3d, desc.getVolatility(), delta);
		
		assertEquals(0.3d, desc.recalcVolatility(504.0d), delta);
		assertEquals(0.3d, desc.getVolatility(), delta);
	}

}
