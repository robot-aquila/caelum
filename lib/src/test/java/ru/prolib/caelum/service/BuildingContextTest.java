package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import javax.servlet.Servlet;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;

public class BuildingContextTest {
	IMocksControl control;
	AbstractConfig configMock;
	CompositeService servicesMock;
	ServletRegistry servletsMock;
	IService serviceMock;
	Servlet svtMock;
	ServletMapping servletMock;
	ICaelum caelumMock;
	BuildingContext service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(AbstractConfig.class);
		servicesMock = control.createMock(CompositeService.class);
		servletsMock = control.createMock(ServletRegistry.class);
		serviceMock = control.createMock(IService.class);
		servletMock = control.createMock(ServletMapping.class);
		svtMock = control.createMock(Servlet.class);
		caelumMock = control.createMock(ICaelum.class);
		service = new BuildingContext(configMock, "/foo/bar", "/foo/foo", caelumMock, servicesMock, servletsMock);
	}
	
	@Test
	public void testCtor6() {
		assertSame(configMock, service.getConfig());
		assertEquals("/foo/bar", service.getDefaultConfigFileName());
		assertEquals("/foo/foo", service.getConfigFileName());
		assertSame(caelumMock, service.getCaelum());
		assertSame(servicesMock, service.getServices());
		assertSame(servletsMock, service.getServlets());
	}
	
	@Test
	public void testCtor3() {
		service = new BuildingContext(configMock, "/foo/bar", "/foo/gap");
		assertSame(configMock, service.getConfig());
		assertEquals("/foo/bar", service.getDefaultConfigFileName());
		assertEquals("/foo/gap", service.getConfigFileName());
		// Do not ask for Caelum because of exception
		assertNotNull(service.getServices());
		assertNotNull(service.getServlets());
	}
	
	@Test
	public void testCtor4() {
		service = new BuildingContext(configMock, "/foo/bar", "/foo/gap", caelumMock);
		assertSame(configMock, service.getConfig());
		assertEquals("/foo/bar", service.getDefaultConfigFileName());
		assertEquals("/foo/gap", service.getConfigFileName());
		assertSame(caelumMock, service.getCaelum());
		assertNotNull(service.getServices());
		assertNotNull(service.getServlets());
	}
	
	@Test
	public void testCtor5_ShouldThrowIfDefaultConfigFileNameIsNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> new BuildingContext(configMock, null, "/foo/foo", caelumMock, servicesMock, servletsMock));
		
		assertEquals("Default config file name cannot be null", e.getMessage());
	}
	
	@Test
	public void testCtor3_ShouldThrowIfDefaultConfigFileNameIsNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> new BuildingContext(configMock, null, "/foo/foo"));
		
		assertEquals("Default config file name cannot be null", e.getMessage());
	}
	
	@Test
	public void testCtor4_ShouldThrowIfDefaultConfigFileNameIsNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> new BuildingContext(configMock, null, "/foo/foo", caelumMock));
		
		assertEquals("Default config file name cannot be null", e.getMessage());
	}
	
	@Test
	public void testGetCaelum_ShouldThrowIfNotDefined() {
		service = new BuildingContext(configMock, "/bar/bar", "/foo/foo");
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getCaelum());
		
		assertEquals("Caelum service was not defined", e.getMessage());
	}
	
	@Test
	public void testRegisterService() {
		expect(servicesMock.register(serviceMock)).andReturn(servicesMock);
		control.replay();
		
		assertSame(service, service.registerService(serviceMock));
		
		control.verify();
	}
	
	@Test
	public void testRegisterServlet1() {
		expect(servletsMock.registerServlet(servletMock)).andReturn(servletsMock);
		control.replay();
		
		assertSame(service, service.registerServlet(servletMock));
		
		control.verify();
	}
	
	@Test
	public void testRegisterServlet2() {
		expect(servletsMock.registerServlet(svtMock, "/bar/*")).andReturn(servletsMock);
		control.replay();
		
		assertSame(service, service.registerServlet(svtMock, "/bar/*"));
		
		control.verify();
	}
	
	@Test
	public void testWithCaelum() {
		service = new BuildingContext(configMock, "foo", "bar");
		
		BuildingContext actual = service.withCaelum(caelumMock);
		
		assertNotNull(actual);
		assertNotSame(service, actual);
		assertSame(caelumMock, actual.getCaelum());
	}

}
