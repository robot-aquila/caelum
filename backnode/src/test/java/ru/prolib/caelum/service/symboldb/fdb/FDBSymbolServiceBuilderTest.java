package ru.prolib.caelum.service.symboldb.fdb;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.easymock.Capture;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICategoryExtractor;
import ru.prolib.caelum.service.symboldb.ISymbolService;

public class FDBSymbolServiceBuilderTest {
	
	static class TestCategoryExtractor implements ICategoryExtractor {

		@Override
		public Collection<String> extract(String symbol) {
			throw new UnsupportedOperationException();
		}
		
	}
	
	IMocksControl control;
	FDB fdbMock;
	Database dbMock;
	File fileMock;
	FDBSymbolServiceConfig configStub, mockedConfig;
	IBuildingContext contextMock;
	FDBSymbolServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		fdbMock = control.createMock(FDB.class);
		dbMock = control.createMock(Database.class);
		fileMock = control.createMock(File.class);
		configStub = new FDBSymbolServiceConfig();
		contextMock = control.createMock(IBuildingContext.class);
		service = new FDBSymbolServiceBuilder();
		mockedService = partialMockBuilder(FDBSymbolServiceBuilder.class)
				.withConstructor()
				.addMockedMethod("createConfig")
				.createMock();
		mockedConfig = partialMockBuilder(FDBSymbolServiceConfig.class)
				.withConstructor()
				.addMockedMethod("load", String.class, String.class)
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		FDBSymbolServiceConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateSchema() {
		FDBSchema actual = service.createSchema("foobar");
		
		assertNotNull(actual);
		assertEquals(new Subspace(Tuple.from("foobar")), actual.getSpace());
	}
	
	@Test
	public void testCreateCategoryExtractor() throws Exception {
		ICategoryExtractor actual = service.createCategoryExtractor(TestCategoryExtractor.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestCategoryExtractor.class)));
	}
	
	@Test
	public void testBuild() throws Exception {
		Properties props = mockedConfig.getProperties();
		props.put("caelum.symboldb.category.extractor", TestCategoryExtractor.class.getName());
		props.put("caelum.symboldb.list.symbols.limit", "2000");
		props.put("caelum.symboldb.fdb.subspace", "zulu24");
		props.put("caelum.symboldb.fdb.cluster", "gamurappi");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("/foo/default.props");
		expect(contextMock.getConfigFileName()).andStubReturn("/foo/my.props");
		mockedConfig.load("/foo/default.props", "/foo/my.props");
		Capture<FDBDatabaseService> cap3 = newCapture();
		expect(contextMock.registerService(capture(cap3))).andReturn(contextMock);
		control.replay();
		replay(mockedService);
		replay(mockedConfig);
		
		ISymbolService actual = mockedService.build(contextMock);
		
		verify(mockedConfig);
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(FDBSymbolService.class)));
		// Test produced symbol service
		FDBSymbolService produced_service = (FDBSymbolService) actual;
		assertThat(produced_service.getCategoryExtractor(), is(instanceOf(TestCategoryExtractor.class)));
		assertEquals(new FDBSchema(new Subspace(Tuple.from("zulu24"))), produced_service.getSchema());
		assertEquals(2000, produced_service.getListSymbolsMaxLimit());
		assertNull(produced_service.getDatabase());
		// Test registered service
		FDBDatabaseService reg_svc = cap3.getValue();
		assertSame(produced_service, reg_svc.getTarget());
		assertEquals("gamurappi", reg_svc.getFdbCluster());
	}
	
	@Test
	public void testHashCode() {
		int expected = 2011572;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBSymbolServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
