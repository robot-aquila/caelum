package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;

import static org.hamcrest.Matchers.*;

import org.junit.Before;
import org.junit.Test;

public class CommonCategoryExtractorTest {
	CommonCategoryExtractor service;

	@Before
	public void setUp() throws Exception {
		service = new CommonCategoryExtractor();
	}
	
	@Test
	public void testGetInstance() {
		ICategoryExtractor actual = CommonCategoryExtractor.getInstance();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(CommonCategoryExtractor.class)));
		assertSame(actual, CommonCategoryExtractor.getInstance());
	}
	
	@Test
	public void testExtract() {
		assertEquals(Arrays.asList(""), service.extract("bumba"));
		assertEquals(Arrays.asList(""), service.extract("@bumba"));
		assertEquals(Arrays.asList("foo"), service.extract("foo@bar"));
	}
	
	@Test
	public void testHashCode() {
		int expected = 1726900123;
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(CommonCategoryExtractor.getInstance()));
		assertTrue(service.equals(new CommonCategoryExtractor()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
