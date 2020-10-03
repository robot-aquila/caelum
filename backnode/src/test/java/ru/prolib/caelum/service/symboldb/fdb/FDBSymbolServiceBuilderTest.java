package ru.prolib.caelum.service.symboldb.fdb;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.util.Collection;

import org.easymock.Capture;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.service.GeneralConfig;
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
	GeneralConfig configMock;
	IBuildingContext contextMock;
	FDBSymbolServiceBuilder service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		fdbMock = control.createMock(FDB.class);
		dbMock = control.createMock(Database.class);
		fileMock = control.createMock(File.class);
		configMock = control.createMock(GeneralConfig.class);
		contextMock = control.createMock(IBuildingContext.class);
		service = new FDBSymbolServiceBuilder();
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
		expect(configMock.getSymbolServiceCategoryExtractor()).andStubReturn(TestCategoryExtractor.class.getName());
		expect(configMock.getMaxSymbolsLimit()).andStubReturn(2000);
		expect(configMock.getMaxEventsLimit()).andStubReturn(3000);
		expect(configMock.getFdbSubspace()).andStubReturn("zulu24");
		expect(configMock.getFdbCluster()).andStubReturn("gamurappi");
		expect(contextMock.getConfig()).andReturn(configMock);
		Capture<FDBDatabaseService> cap3 = newCapture();
		expect(contextMock.registerService(capture(cap3))).andReturn(contextMock);
		control.replay();
		
		ISymbolService actual = service.build(contextMock);
		
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
