package ru.prolib.caelum.service.itemdb;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Collection;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.service.GeneralConfig;
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
	GeneralConfig configMock;
	IBuildingContext contextMock;
	ItemDatabaseServiceBuilder service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(GeneralConfig.class);
		contextMock = control.createMock(IBuildingContext.class);
		service = new ItemDatabaseServiceBuilder();
	}
	
	@Test
	public void testCreateBuilder() throws Exception {
		IItemDatabaseServiceBuilder actual = service.createBuilder(TestBuilder.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		expect(contextMock.getConfig()).andReturn(configMock);
		expect(configMock.getItemServiceBuilder()).andReturn(TestBuilder.class.getName());
		control.replay();

		IItemDatabaseService actual = service.build(contextMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
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
