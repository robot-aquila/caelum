package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;

public class ItemDatabaseServiceBuilderTest {
	IMocksControl control;
	ItemDatabaseConfig configMock;
	ItemDatabaseService serviceMock;
	ItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(ItemDatabaseConfig.class);
		serviceMock = control.createMock(ItemDatabaseService.class);
		mockedService = partialMockBuilder(ItemDatabaseServiceBuilder.class)
				.addMockedMethod("createConfig")
				.addMockedMethod("createService")
				.createMock();
		service = new ItemDatabaseServiceBuilder();
	}
	
	@Test
	public void testCreateConfig() {
		ItemDatabaseConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateService() {
		ItemDatabaseService actual = service.createService(configMock);
		
		assertNotNull(actual);
		assertSame(configMock, actual.getConfig());
	}
	
	@Test
	public void testBuild() throws Exception {
		expect(mockedService.createConfig()).andReturn(configMock);
		configMock.load("tutumbr.props");
		expect(mockedService.createService(configMock)).andReturn(serviceMock);
		replay(mockedService);
		
		IItemDatabaseService actual = mockedService.build("tutumbr.props", new CompositeService());
		
		verify(mockedService);
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemDatabaseService.class)));
	}
	
	@Test
	public void testHashCode() {
		int expected = 50098172;
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ItemDatabaseServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
