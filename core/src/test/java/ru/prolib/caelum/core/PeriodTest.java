package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class PeriodTest {

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void test() {
		List<Period> list = new ArrayList<>();
		for ( String code : Arrays.asList("M1", "M2", "M3", "M5", "M6", "M10", "M12",
				"M15", "M20", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D1") )
		{
			Period p = Period.valueOf(code);
			assertNotNull(p);
			list.add(p);
		}
		assertEquals(list, Arrays.asList(Period.values()));
	}

}
