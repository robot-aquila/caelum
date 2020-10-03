package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import javax.servlet.Servlet;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.IService;

public class BuildingContextTest {
	IMocksControl control;
	GeneralConfig configMock1, configMock2;
	IServiceRegistry servicesMock1, servicesMock2;
	IServletRegistry servletsMock1, servletsMock2;
	IService serviceMock;
	Servlet svtMock;
	ServletMapping servletMock;
	ICaelum caelumMock1, caelumMock2;
	BuildingContext service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock1 = control.createMock(GeneralConfig.class);
		configMock2 = control.createMock(GeneralConfig.class);
		servicesMock1 = control.createMock(IServiceRegistry.class);
		servicesMock2 = control.createMock(IServiceRegistry.class);
		servletsMock1 = control.createMock(IServletRegistry.class);
		servletsMock2 = control.createMock(IServletRegistry.class);
		serviceMock = control.createMock(IService.class);
		servletMock = control.createMock(ServletMapping.class);
		svtMock = control.createMock(Servlet.class);
		caelumMock1 = control.createMock(ICaelum.class);
		caelumMock2 = control.createMock(ICaelum.class);
		service = new BuildingContext(configMock1, "/foo/bar", "/foo/foo", caelumMock1, servicesMock1, servletsMock1);
	}
		
	@Test
	public void testCtor6() {
		assertSame(configMock1, service.getConfig());
		assertEquals("/foo/bar", service.getDefaultConfigFileName());
		assertEquals("/foo/foo", service.getConfigFileName());
		assertSame(caelumMock1, service.getCaelum());
		assertSame(servicesMock1, service.getServices());
		assertSame(servletsMock1, service.getServlets());
	}
	
	@Test
	public void testCtor6_ShouldThrowIfDefaultConfigFileNameIsNull() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> new BuildingContext(configMock1, null, "/foo/foo", caelumMock1, servicesMock1, servletsMock1));
		
		assertEquals("Default config file name cannot be null", e.getMessage());
	}
	
	@Test
	public void testRegisterService() {
		expect(servicesMock1.registerService(serviceMock)).andReturn(servicesMock1);
		control.replay();
		
		assertSame(service, service.registerService(serviceMock));
		
		control.verify();
	}
	
	@Test
	public void testRegisterServlet1() {
		expect(servletsMock1.registerServlet(servletMock)).andReturn(servletsMock1);
		control.replay();
		
		assertSame(service, service.registerServlet(servletMock));
		
		control.verify();
	}
	
	@Test
	public void testRegisterServlet2() {
		expect(servletsMock1.registerServlet(svtMock, "/bar/*")).andReturn(servletsMock1);
		control.replay();
		
		assertSame(service, service.registerServlet(svtMock, "/bar/*"));
		
		control.verify();
	}
	
	@Test
	public void testWithCaelum() {
		service = new BuildingContext(configMock1, "foo", "bar", null, servicesMock1, servletsMock1);
		
		BuildingContext actual = service.withCaelum(caelumMock1);
		
		assertNotNull(actual);
		assertNotSame(service, actual);
		assertSame(configMock1, actual.getConfig());
		assertEquals("foo", actual.getDefaultConfigFileName());
		assertEquals("bar", actual.getConfigFileName());
		assertSame(caelumMock1, actual.getCaelum());
		assertSame(servicesMock1, actual.getServices());
		assertSame(servletsMock1, actual.getServlets());
	}
	
	@Test
	public void testWithConfig() {
		service = new BuildingContext(configMock1, "foo", "bar", caelumMock1, servicesMock1, servletsMock1);
		
		BuildingContext actual = service.withConfig(configMock2);
		
		assertNotNull(actual);
		assertNotSame(service, actual);
		assertSame(configMock2, actual.getConfig());
		assertEquals("foo", actual.getDefaultConfigFileName());
		assertEquals("bar", actual.getConfigFileName());
		assertSame(caelumMock1, actual.getCaelum());
		assertSame(servicesMock1, actual.getServices());
		assertSame(servletsMock1, actual.getServlets());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(
				new BuildingContext(configMock1, "/foo/bar", "/foo/foo", caelumMock1, servicesMock1, servletsMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(
				new BuildingContext(configMock2, "/foo/bar", "/foo/foo", caelumMock1, servicesMock1, servletsMock1)));
		assertFalse(service.equals(
				new BuildingContext(configMock1, "/bar/bar", "/foo/foo", caelumMock1, servicesMock1, servletsMock1)));
		assertFalse(service.equals(
				new BuildingContext(configMock1, "/foo/bar", "/bar/foo", caelumMock1, servicesMock1, servletsMock1)));
		assertFalse(service.equals(
				new BuildingContext(configMock1, "/foo/bar", "/foo/foo", caelumMock2, servicesMock1, servletsMock1)));
		assertFalse(service.equals(
				new BuildingContext(configMock1, "/foo/bar", "/foo/foo", caelumMock1, servicesMock2, servletsMock1)));
		assertFalse(service.equals(
				new BuildingContext(configMock1, "/foo/bar", "/foo/foo", caelumMock1, servicesMock1, servletsMock2)));
		assertFalse(service.equals(
				new BuildingContext(configMock2, "/bar/bar", "/bar/foo", caelumMock2, servicesMock2, servletsMock2)));
		
	}

}
