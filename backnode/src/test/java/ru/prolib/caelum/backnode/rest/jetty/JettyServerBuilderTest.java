package ru.prolib.caelum.backnode.rest.jetty;

import static org.junit.Assert.*;

import javax.servlet.Servlet;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.easymock.IMocksControl;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.BacknodeConfig;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.BuildingContext;
import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ExtensionStub;
import ru.prolib.caelum.service.IExtension;
import ru.prolib.caelum.service.ServletRegistry;

public class JettyServerBuilderTest {
	IMocksControl control;
	IService serviceMock;
	Server jserverMock;
	ServletContextHandler ctxhMock;
	BacknodeConfig mockedConfig;
	BuildingContext contextMock;
	Servlet servletMock1, servletMock2, servletMock3;
	ServletRegistry servlets;
	ServletHolder sholderMock1, sholderMock2, sholderMock3;
	JettyServerBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		serviceMock = control.createMock(IService.class);
		jserverMock = control.createMock(Server.class);
		ctxhMock = control.createMock(ServletContextHandler.class);
		contextMock = control.createMock(BuildingContext.class);
		servletMock1 = control.createMock(Servlet.class);
		servletMock2 = control.createMock(Servlet.class);
		servletMock3 = control.createMock(Servlet.class);
		sholderMock1 = control.createMock(ServletHolder.class);
		sholderMock2 = control.createMock(ServletHolder.class);
		sholderMock3 = control.createMock(ServletHolder.class);
		servlets = new ServletRegistry();
		service = new JettyServerBuilder();
		mockedService = partialMockBuilder(JettyServerBuilder.class)
				.withConstructor()
				.addMockedMethod("createConfig")
				.addMockedMethod("createContextHandler")
				.addMockedMethod("createJettyServer", String.class, int.class)
				.addMockedMethod("createServletHolder", Servlet.class)
				.addMockedMethod("createServer", String.class, int.class, ServletRegistry.class)
				.createMock(control);
		mockedConfig = partialMockBuilder(BacknodeConfig.class)
				.withConstructor()
				.addMockedMethod("load", String.class, String.class)
				.createMock(control);
	}
	
	@Test
	public void testCreateConfig() {
		BacknodeConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateContextHandler() {
		ServletContextHandler actual = service.createContextHandler();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateJettyServer() {
		Server actual = service.createJettyServer("localhost", 5678);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateServletHolder() throws Exception {
		ServletHolder actual = service.createServletHolder(servletMock1);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateServer() {
		mockedService = partialMockBuilder(JettyServerBuilder.class)
				.withConstructor()
				.addMockedMethod("createContextHandler")
				.addMockedMethod("createJettyServer", String.class, int.class)
				.addMockedMethod("createServletHolder", Servlet.class)
				.createMock(control);
		expect(mockedService.createContextHandler()).andReturn(ctxhMock);
		ctxhMock.setContextPath("/");
		expect(mockedService.createJettyServer("mutabor", 2567)).andReturn(jserverMock);
		jserverMock.setHandler(ctxhMock);
		expect(mockedService.createServletHolder(servletMock1)).andReturn(sholderMock1);
		ctxhMock.addServlet(sholderMock1, "/foo/*");
		expect(mockedService.createServletHolder(servletMock2)).andReturn(sholderMock2);
		ctxhMock.addServlet(sholderMock2, "/bar/24");
		expect(mockedService.createServletHolder(servletMock3)).andReturn(sholderMock3);
		ctxhMock.addServlet(sholderMock3, "/gap/pop");
		control.replay();
		servlets.registerServlet(servletMock1, "/foo/*");
		servlets.registerServlet(servletMock2, "/bar/24");
		servlets.registerServlet(servletMock3, "/gap/pop");
		
		IService actual = mockedService.createServer("mutabor", 2567, servlets);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(JettyServerStarter.class)));
		assertSame(jserverMock, ((JettyServerStarter) actual).getServer());
	}

	@Test
	public void testBuild() throws Exception {
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("foo.props");
		expect(contextMock.getConfigFileName()).andStubReturn("bar.props");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		mockedConfig.load("foo.props", "bar.props");
		expect(contextMock.getServlets()).andReturn(servlets);
		expect(mockedService.createServer("bambr1", 7281, servlets)).andReturn(serviceMock);
		expect(contextMock.registerService(serviceMock)).andReturn(contextMock);
		control.replay();
		mockedConfig.getProperties().put("caelum.backnode.rest.http.host", "bambr1");
		mockedConfig.getProperties().put("caelum.backnode.rest.http.port", "7281");
		
		IExtension actual = mockedService.build(contextMock);

		control.verify();
		assertEquals(new ExtensionStub(new ExtensionStatus("HTTP", ExtensionState.RUNNING, null)), actual);
	}

}
