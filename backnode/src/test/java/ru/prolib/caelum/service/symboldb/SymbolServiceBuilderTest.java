package ru.prolib.caelum.service.symboldb;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collection;

import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.service.EventListRequest;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.SymbolListRequest;

public class SymbolServiceBuilderTest {
	
	static class TestService implements ISymbolService {
		private final IBuildingContext context;
		
		public TestService(IBuildingContext context) {
			this.context = context;
		}

		@Override
		public void registerSymbol(String symbol) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void registerSymbol(Collection<String> symbols) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void registerEvents(Events events) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void registerEvents(Collection<Events> events) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void deleteEvents(Events events) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void deleteEvents(Collection<Events> events) {
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
		public ICloseableIterator<Events> listEvents(EventListRequest request) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void clear(boolean global) {
			throw new UnsupportedOperationException();
		}
		
	}
	
	static class TestBuilder implements ISymbolServiceBuilder {

		@Override
		public ISymbolService build(IBuildingContext context) throws IOException {
			return new TestService(context);
		}
		
	}
	
	IMocksControl control;
	SymbolServiceConfig configMock;
	IBuildingContext contextMock;
	SymbolServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(SymbolServiceConfig.class);
		contextMock = control.createMock(IBuildingContext.class);
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
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("/bar/bums.defaults");
		expect(contextMock.getConfigFileName()).andStubReturn("/bar/bums.props");
		configMock.load("/bar/bums.defaults", "/bar/bums.props");
		expect(configMock.getString("caelum.symboldb.builder")).andReturn(TestBuilder.class.getName());
		expect(mockedService.createConfig()).andReturn(configMock);
		control.replay();
		replay(mockedService);
		
		ISymbolService actual = mockedService.build(contextMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertSame(contextMock, x.context);
	}
	
	@Test
	public void testHashCode() {
		int expected = 5578912;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
