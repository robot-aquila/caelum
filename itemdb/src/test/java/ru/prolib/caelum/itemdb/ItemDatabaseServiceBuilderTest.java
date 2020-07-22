package ru.prolib.caelum.itemdb;

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
import ru.prolib.caelum.core.IItem;

public class ItemDatabaseServiceBuilderTest {
	
	static class TestService implements IItemDatabaseService {
		private final String default_config_file, config_file;
		private final CompositeService services;
		
		public TestService(String default_config_file, String config_file, CompositeService services) {
			this.default_config_file = default_config_file;
			this.config_file = config_file;
			this.services = services;
		}

		@Override
		public IItemIterator fetch(ItemDataRequest request) {
			throw new UnsupportedOperationException();
		}

		@Override
		public IItemIterator fetch(ItemDataRequestContinue request) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void registerItem(IItem item) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void clear() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	static class TestBuilder implements IItemDatabaseServiceBuilder {

		@Override
		public IItemDatabaseService build(String default_config_file, String config_file, CompositeService services)
				throws IOException
		{
			return new TestService(default_config_file, config_file, services);
		}
		
	}
	
	IMocksControl control;
	ItemDatabaseConfig configStub;
	CompositeService servicesMock;
	ItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configStub = new ItemDatabaseConfig();
		servicesMock = control.createMock(CompositeService.class);
		service = new ItemDatabaseServiceBuilder();
		mockedService = partialMockBuilder(ItemDatabaseServiceBuilder.class)
				.addMockedMethod("createConfig")
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		ItemDatabaseConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateBuilder() throws Exception {
		IItemDatabaseServiceBuilder actual = service.createBuilder(TestBuilder.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		final Capture<String> cap1 = newCapture(), cap2 = newCapture();
		configStub = new ItemDatabaseConfig() {
			@Override
			public void load(String default_props_file, String props_file) {
				cap1.setValue(default_props_file);
				cap2.setValue(props_file);
			}
		};
		Properties props = configStub.getProperties();
		props.put("caelum.itemdb.builder", TestBuilder.class.getName());
		expect(mockedService.createConfig()).andReturn(configStub);
		control.replay();
		replay(mockedService);

		IItemDatabaseService actual = mockedService.build("/jumbo/foo.props", "/jumbo/bar.props", servicesMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertEquals("/jumbo/foo.props", x.default_config_file);
		assertEquals("/jumbo/bar.props", x.config_file);
		assertSame(servicesMock, x.services);
	}

	@Test
	public void testHashCode() {
		int expected = 998102811;
		
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
