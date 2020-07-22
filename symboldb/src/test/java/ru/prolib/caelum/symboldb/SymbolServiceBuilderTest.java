package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import java.io.IOException;
import java.util.Properties;

import org.easymock.Capture;
import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.ICloseableIterator;

public class SymbolServiceBuilderTest {
	
	static class TestService implements ISymbolService {
		private final String default_config_file, config_file;
		private final CompositeService services;
		
		public TestService(String default_config_file, String config_file, CompositeService services) {
			this.default_config_file = default_config_file;
			this.config_file = config_file;
			this.services = services;
		}

		@Override
		public void registerSymbol(String symbol) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void registerSymbolUpdate(SymbolUpdate update) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ICloseableIterator<String> listCategories() {
			throw new UnsupportedOperationException();
		}

		@Override
		public ICloseableIterator<String> listSymbols(SymbolListRequest request) {
			throw new UnsupportedOperationException();		}

		@Override
		public ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	static class TestBuilder implements ISymbolServiceBuilder {

		@Override
		public ISymbolService build(String default_config_file, String config_file, CompositeService services)
				throws IOException
		{
			return new TestService(default_config_file, config_file, services);
		}
		
	}
	
	IMocksControl control;
	SymbolServiceConfig configStub;
	CompositeService servicesMock;
	SymbolServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configStub = new SymbolServiceConfig();
		servicesMock = control.createMock(CompositeService.class);
		service = new SymbolServiceBuilder();
		mockedService = partialMockBuilder(SymbolServiceBuilder.class)
				.addMockedMethod("createConfig")
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		SymbolServiceConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateBuilder() throws Exception {
		ISymbolServiceBuilder actual = service.createBuilder(TestBuilder.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		final Capture<String> cap1 = newCapture(), cap2 = newCapture();
		configStub = new SymbolServiceConfig() {
			@Override
			public void load(String default_props_file, String props_file) {
				cap1.setValue(default_props_file);
				cap2.setValue(props_file);
			}
		};
		Properties props = configStub.getProperties();
		props.put("caelum.symboldb.builder", TestBuilder.class.getName());
		expect(mockedService.createConfig()).andReturn(configStub);
		control.replay();
		replay(mockedService);
		
		ISymbolService actual = mockedService.build("/bar/bums.defaults", "/bar/bums.props", servicesMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertEquals("/bar/bums.defaults", x.default_config_file);
		assertEquals("/bar/bums.props", x.config_file);
		assertSame(servicesMock, x.services);
	}
	
	@Test
	public void testHashCode() {
		int expected = 5578912;
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
