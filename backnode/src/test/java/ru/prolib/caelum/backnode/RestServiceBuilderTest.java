package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import javax.servlet.Servlet;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ExtensionStub;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.IExtension;

public class RestServiceBuilderTest {
	IMocksControl control;
	IBuildingContext contextMock;
	Servlet servletMock1, servletMock2;
	ICaelum caelumMock;
	BacknodeConfig mockedConfig;
	RestServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		contextMock = control.createMock(IBuildingContext.class);
		servletMock1 = control.createMock(Servlet.class);
		servletMock2 = control.createMock(Servlet.class);
		caelumMock = control.createMock(ICaelum.class);
		mockedConfig = partialMockBuilder(BacknodeConfig.class)
				.withConstructor()
				.addMockedMethod("load", String.class, String.class)
				.createMock();
		service = new RestServiceBuilder();
		mockedService = partialMockBuilder(RestServiceBuilder.class)
				.withConstructor()
				.addMockedMethod("createConfig")
				.addMockedMethod("createConsoleStaticFilesServlet")
				.addMockedMethod("createRestServiceComponent", ICaelum.class, boolean.class)
				.addMockedMethod("createServletContainer", ResourceConfig.class)
				.addMockedMethod("createRestServiceServlet", ICaelum.class, boolean.class)
				.createMock();
				
	}
	
	@Test
	public void testCreateConfig() {
		BacknodeConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testRestServiceComponent() {
		Object actual;
		
		actual = service.createRestServiceComponent(caelumMock, true);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(RestService.class)));
		RestService x = (RestService) actual;
		assertSame(caelumMock, x.getCaelum());
		assertNotNull(x.getStreamFactory());
		assertNotNull(x.getIntervals());
		assertNotNull(x.getByteUtils());
		assertTrue(x.isTestMode());
		
		actual = service.createRestServiceComponent(caelumMock, false);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(RestService.class)));
		x = (RestService) actual;
		assertSame(caelumMock, x.getCaelum());
		assertNotNull(x.getStreamFactory());
		assertNotNull(x.getIntervals());
		assertNotNull(x.getByteUtils());
		assertFalse(x.isTestMode());
	}
	
	@Test
	public void testCreateConsoleStaticFilesServlet() {
		Servlet actual = service.createConsoleStaticFilesServlet();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StaticResourceServlet.class)));
	}
	
	@Test
	public void testCreateServletContainer() {
		ResourceConfig rc = control.createMock(ResourceConfig.class);
		
		Servlet actual = service.createServletContainer(rc);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ServletContainer.class)));
		// Unable to test more because no public access to RC.
	}
	
	@Test
	public void testCreateRestServiceServlet() {
		Capture<ResourceConfig> rcap = newCapture();
		RestService restService = (RestService) service.createRestServiceComponent(caelumMock, false);
		mockedService = partialMockBuilder(RestServiceBuilder.class)
				.withConstructor()
				.addMockedMethod("createRestServiceComponent", ICaelum.class, boolean.class)
				.addMockedMethod("createServletContainer", ResourceConfig.class)
				.createMock();
		expect(mockedService.createRestServiceComponent(caelumMock, false)).andReturn(restService);
		expect(mockedService.createServletContainer(capture(rcap))).andReturn(servletMock1);
		control.replay();
		replay(mockedService);
		
		Servlet actual = mockedService.createRestServiceServlet(caelumMock, false);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertSame(servletMock1, actual);
		
		// Test the resource config captured
		ResourceConfig rc = rcap.getValue();
		assertThat(rc, is(instanceOf(CommonResourceConfig.class)));
		assertTrue(rc.isRegistered(restService));
	}

	@Test
	public void testBuild() throws Exception {
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("foo.props");
		expect(contextMock.getConfigFileName()).andStubReturn("bar.props");
		expect(contextMock.getCaelum()).andStubReturn(caelumMock);
		mockedConfig.getProperties().setProperty("caelum.backnode.mode", "prod");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		mockedConfig.load("foo.props", "bar.props");
		expect(mockedService.createConsoleStaticFilesServlet()).andReturn(servletMock1);
		expect(contextMock.registerServlet(servletMock1, "/console/*")).andReturn(contextMock);
		expect(mockedService.createRestServiceServlet(caelumMock, false)).andReturn(servletMock2);
		expect(contextMock.registerServlet(servletMock2, "/*")).andReturn(contextMock);
		control.replay();
		replay(mockedConfig);
		replay(mockedService);
		
		IExtension actual = mockedService.build(contextMock);
		
		verify(mockedService);
		verify(mockedConfig);
		control.verify();
		assertEquals(new ExtensionStub(new ExtensionStatus("REST", ExtensionState.RUNNING, null)), actual);
	}

}
