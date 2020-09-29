package ru.prolib.caelum.service.itemdb;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.ItemDataRequest;
import ru.prolib.caelum.service.ItemDataRequestContinue;

public class ItemDatabaseServiceBuilderTest {
	
	static class TestService implements IItemDatabaseService {
		private final IBuildingContext context;
		
		public TestService(IBuildingContext context) {
			this.context = context;
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
		public void registerItem(Collection<IItem> items) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void registerItem(IItem item) {
			throw new UnsupportedOperationException();
		}
		
		@Override
		public void clear(boolean global) {
			throw new UnsupportedOperationException();
		}
		
	}
	
	static class TestBuilder implements IItemDatabaseServiceBuilder {

		@Override
		public IItemDatabaseService build(IBuildingContext context) throws IOException {
			return new TestService(context);
		}
		
	}
	
	IMocksControl control;
	ItemDatabaseConfig configStub;
	IBuildingContext contextMock;
	ItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configStub = new ItemDatabaseConfig();
		contextMock = control.createMock(IBuildingContext.class);
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
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("/jumbo/foo.props");
		expect(contextMock.getConfigFileName()).andStubReturn("/jumbo/bar.props");
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

		IItemDatabaseService actual = mockedService.build(contextMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertEquals("/jumbo/foo.props", cap1.getValue());
		assertEquals("/jumbo/bar.props", cap2.getValue());
		assertSame(contextMock, x.context);
	}

	@Test
	public void testHashCode() {
		int expected = 998102811;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ItemDatabaseServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}
	
}
