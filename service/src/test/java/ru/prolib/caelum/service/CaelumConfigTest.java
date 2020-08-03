package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class CaelumConfigTest {
	CaelumConfig service;

	@Before
	public void setUp() throws Exception {
		service = new CaelumConfig();
	}
	
	@Test
	public void testGetDefaultConfigFile() {
		assertEquals("app.caelum.properties", service.getDefaultConfigFile());
	}

	@Test
	public void testGetExtensions() {
		service.getProperties().put("caelum.extension.builder.001_Svc1", "ServiceBuilder1");
		service.getProperties().put("caelum.extension.enabled.001_Svc1", "true");
		service.getProperties().put("caelum.extension.builder.002_Svc2", "ServiceBuilder2");
		service.getProperties().put("caelum.extension.enabled.002_Svc2", "false");
		
		List<ExtensionConf> actual = service.getExtensions();
		
		List<ExtensionConf> expected = Arrays.asList(
				new ExtensionConf("001_Svc1", "ServiceBuilder1", true),
				new ExtensionConf("002_Svc2", "ServiceBuilder2", false)
			);
		assertEquals(expected, actual);
	}

}
