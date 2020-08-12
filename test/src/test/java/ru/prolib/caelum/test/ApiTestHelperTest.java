package ru.prolib.caelum.test;

import static org.junit.Assert.*;
import static ru.prolib.caelum.test.ApiTestHelper.*;

import java.math.BigDecimal;

import org.junit.Before;
import org.junit.Test;

public class ApiTestHelperTest {

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testToBD_ShouldKeepTrailingZeros() {
		BigDecimal x = toBD(27280, 3);
		
		assertEquals(3, x.scale());
		assertEquals("27.280", x.toPlainString());
	}

}
