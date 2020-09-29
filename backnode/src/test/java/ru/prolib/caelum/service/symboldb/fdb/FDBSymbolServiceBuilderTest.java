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

import ru.prolib.caelum.lib.CompositeService;
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
	FDBSymbolServiceConfig configStub;
	CompositeService servicesMock;
	FDBSymbolServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		fdbMock = control.createMock(FDB.class);
		dbMock = control.createMock(Database.class);
		fileMock = control.createMock(File.class);
		configStub = new FDBSymbolServiceConfig();
		servicesMock = control.createMock(CompositeService.class);
		service = new FDBSymbolServiceBuilder();
		mockedService = partialMockBuilder(FDBSymbolServiceBuilder.class)
				.addMockedMethod("createConfig")
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
		final Capture<String> cap1 = newCapture(), cap2 = newCapture();
		configStub = new FDBSymbolServiceConfig() {
			@Override
			public void load(String default_props_file, String props_file) {
				cap1.setValue(default_props_file);
				cap2.setValue(props_file);
			}
		};
		Properties props = configStub.getProperties();
		props.put("caelum.symboldb.category.extractor", TestCategoryExtractor.class.getName());
		props.put("caelum.symboldb.list.symbols.limit", "2000");
		props.put("caelum.symboldb.fdb.subspace", "zulu24");
		props.put("caelum.symboldb.fdb.cluster", "gamurappi");
		expect(mockedService.createConfig()).andReturn(configStub);
		Capture<FDBDatabaseService> cap3 = newCapture();
		expect(servicesMock.register(capture(cap3))).andReturn(servicesMock);
		control.replay();
		replay(mockedService);
		
		ISymbolService actual = mockedService.build("/foo/default.props", "/foo/my.props", servicesMock);
		
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
		// Test configuration loading
		assertEquals("/foo/default.props", cap1.getValue());
		assertEquals("/foo/my.props", cap2.getValue());
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
