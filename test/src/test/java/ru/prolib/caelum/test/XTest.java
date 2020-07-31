package ru.prolib.caelum.test;

import static ru.prolib.caelum.test.ApiTestHelper.*;

import java.math.BigDecimal;

import org.junit.Before;
import org.junit.Test;

public class XTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testX() {
		BigDecimal x = toBD(27280, 3);
		System.out.println("scale: " + x.scale());
		System.out.println("X: " + x.toPlainString());
	}

}
