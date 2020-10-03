package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.BuildingContext;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.ServiceRegistry;
import ru.prolib.caelum.service.ServletRegistry;

public class BacknodeBuilderTest {
	IMocksControl control;
	CompositeService servicesMock;
	CaelumBuilder caelumBuilderMock;
	ServiceRegistry serviceRegistryStub;
	ServletRegistry servletRegistryMock;
	ICaelum caelumMock;
	BacknodeBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		servicesMock = control.createMock(CompositeService.class);
		caelumBuilderMock = control.createMock(CaelumBuilder.class);
		serviceRegistryStub = new ServiceRegistry(servicesMock);
		caelumMock = control.createMock(ICaelum.class);
		service = new BacknodeBuilder();
		mockedService = partialMockBuilder(BacknodeBuilder.class)
				.withConstructor()
				.addMockedMethod("createCaelumBuilder")
				.addMockedMethod("createServiceRegistry")
				.addMockedMethod("createServletRegistry")
				.createMock();
	}
	
	@Test
	public void testCreateCaelumBuilder() {
		CaelumBuilder actual = service.createCaelumBuilder();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateServiceRegistry() {
		ServiceRegistry actual = service.createServiceRegistry();
		
		assertNotNull(actual);
		assertNotNull(actual.getServices());
	}
	
	@Test
	public void testCreateServletRegistry() {
		ServletRegistry actual = service.createServletRegistry();
		
		assertNotNull(actual);
	}

	@Test
	public void testBuild() throws Exception {
		expect(mockedService.createServiceRegistry()).andReturn(serviceRegistryStub);
		expect(mockedService.createCaelumBuilder()).andReturn(caelumBuilderMock);
		expect(mockedService.createServletRegistry()).andStubReturn(servletRegistryMock);
		expect(caelumBuilderMock.build(new BuildingContext(null, "app.caelum-backnode.properties", "bar.props",
				null, serviceRegistryStub, servletRegistryMock))).andReturn(caelumMock);
		control.replay();
		replay(mockedService);
		
		IService actual = mockedService.build("bar.props");
		
		verify(mockedService);
		control.verify();
		assertSame(servicesMock, actual);
	}
	
	@Test
	public void testHashCode() {
		int expected = 8263811;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new BacknodeBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
