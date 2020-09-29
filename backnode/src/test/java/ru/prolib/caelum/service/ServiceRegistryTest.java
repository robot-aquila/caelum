package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;

public class ServiceRegistryTest {
	IMocksControl control;
	CompositeService servicesMock;
	IService serviceMock;
	ServiceRegistry service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		servicesMock = control.createMock(CompositeService.class);
		serviceMock = control.createMock(IService.class);
		service = new ServiceRegistry(servicesMock);
	}
	
	@Test
	public void testCtor1() {
		assertSame(servicesMock, service.getServices());
	}
	
	@Test
	public void testCtor0() {
		service = new ServiceRegistry();
		
		assertNotNull(service.getServices());
	}

	@Test
	public void testRegisterService() {
		expect(servicesMock.register(serviceMock)).andReturn(servicesMock);
		control.replay();
		
		assertSame(service, service.registerService(serviceMock));
		
		control.verify();
	}

}
