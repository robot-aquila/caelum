package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.BasicConfigurator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

import ru.prolib.caelum.core.ServiceException;

public class FDBDatabaseServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	FDB fdbMock;
	Database dbMock;
	File fileMock;
	FDBSymbolService targetMock;
	Writer writerMock;
	FDBDatabaseService service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		fdbMock = control.createMock(FDB.class);
		dbMock = control.createMock(Database.class);
		fileMock = control.createMock(File.class);
		targetMock = control.createMock(FDBSymbolService.class);
		writerMock = control.createMock(Writer.class);
		service = new FDBDatabaseService(targetMock, "fdb.cluster contents");
		mockedService = partialMockBuilder(FDBDatabaseService.class)
				.addMockedMethod("createFDB", int.class)
				.addMockedMethod("createTempFile")
				.addMockedMethod("createWriter", File.class)
				.withConstructor(FDBSymbolService.class, String.class)
				.withArgs(targetMock, "fdb.cluster contents")
				.createMock();
	}
	
	@Test
	public void testGetters() {
		assertSame(targetMock, service.getTarget());
		assertEquals("fdb.cluster contents", service.getFdbCluster());
	}
	
	@Test
	public void testCreateFDB() {
		FDB fdb = service.createFDB(620);
		
		assertNotNull(fdb);
		assertEquals(620, fdb.getAPIVersion());
	}
	
	@Test
	public void testCreateTempFile() throws IOException {
		File actual = service.createTempFile();
		try {
			assertNotNull(actual);
			assertEquals(new File(System.getProperty("java.io.tmpdir")), actual.getParentFile());
			assertTrue(actual.getName().matches("^caelum\\-\\d+\\-fdb\\.cluster$"));
		} finally {
			if ( actual != null ) {
				actual.delete();
			}
		}
	}
	
	@Test
	public void testStart_ThrowsIfStarted() {
		service.setDatabase(dbMock);
		eex.expect(ServiceException.class);
		eex.expectMessage("Service already started");
		
		service.start();
	}
	
	@Test
	public void testStart_WithClusterFile() throws Exception {
		expect(mockedService.createFDB(620)).andReturn(fdbMock);
		expect(mockedService.createTempFile()).andReturn(fileMock);
		expect(fileMock.getAbsolutePath()).andStubReturn("/foo/bumbr.temp");
		fileMock.deleteOnExit();
		expect(mockedService.createWriter(fileMock)).andReturn(writerMock);
		writerMock.write("fdb.cluster contents");
		writerMock.close();
		expect(fdbMock.open("/foo/bumbr.temp")).andReturn(dbMock);
		targetMock.setDatabase(dbMock);
		control.replay();
		replay(mockedService);
		
		mockedService.start();
		
		verify(mockedService);
		control.verify();
		assertSame(dbMock, mockedService.getDatabase());
	}
	
	@Test
	public void testStart_WithoutClusterFile_EmptyString() {
		mockedService = partialMockBuilder(FDBDatabaseService.class)
				.addMockedMethod("createFDB", int.class)
				.withConstructor(FDBSymbolService.class, String.class)
				.withArgs(targetMock, "")
				.createMock();
		expect(mockedService.createFDB(620)).andReturn(fdbMock);
		expect(fdbMock.open()).andReturn(dbMock);
		targetMock.setDatabase(dbMock);
		control.replay();
		replay(mockedService);
		
		mockedService.start();
		
		verify(mockedService);
		control.verify();
		assertSame(dbMock, mockedService.getDatabase());
	}
	
	@Test
	public void testStart_WithoutClusterFile_NullString() {
		mockedService = partialMockBuilder(FDBDatabaseService.class)
				.addMockedMethod("createFDB", int.class)
				.withConstructor(FDBSymbolService.class, String.class)
				.withArgs(targetMock, null)
				.createMock();
		expect(mockedService.createFDB(620)).andReturn(fdbMock);
		expect(fdbMock.open()).andReturn(dbMock);
		targetMock.setDatabase(dbMock);
		control.replay();
		replay(mockedService);
		
		mockedService.start();
		
		verify(mockedService);
		control.verify();
		assertSame(dbMock, mockedService.getDatabase());
	}
	
	@Test
	public void testStop_IfStarted() {
		service.setDatabase(dbMock);
		dbMock.close();
		control.replay();
		
		service.stop();
		service.stop();
		service.stop();
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_IfNotStarted() {
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(800917, 703)
				.append(targetMock)
				.append("fdb.cluster contents")
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		FDBSymbolService targetMock2 = control.createMock(FDBSymbolService.class);
		assertTrue(service.equals(service));
		assertTrue(service.equals(new FDBDatabaseService(targetMock, "fdb.cluster contents")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new FDBDatabaseService(targetMock2, "fdb.cluster contents")));
		assertFalse(service.equals(new FDBDatabaseService(targetMock, "foobar data")));
		assertFalse(service.equals(new FDBDatabaseService(targetMock2, "foobar data")));
	}

}
