package ru.prolib.caelum.backnode.rest.jetty;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.eclipse.jetty.server.Server;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.BacknodeConfig;
import ru.prolib.caelum.backnode.NodeService;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.service.ICaelum;

public class JettyServerBuilderTest {
	IMocksControl control;
	ICaelum caelumMock;
	IService serviceMock;
	BacknodeConfig configStub;
	JettyServerBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		caelumMock = control.createMock(ICaelum.class);
		serviceMock = control.createMock(IService.class);
		service = new JettyServerBuilder();
		mockedService = partialMockBuilder(JettyServerBuilder.class)
				.addMockedMethod("createConfig")
				.addMockedMethod("createComponent", ICaelum.class)
				.addMockedMethod("createServer", String.class, int.class, Object.class)
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		BacknodeConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateComponent() {
		Object actual = service.createComponent(caelumMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(NodeService.class)));
		NodeService x = (NodeService) actual;
		assertSame(caelumMock, x.getCaelum());
		assertNotNull(x.getFreemarker());
		assertNotNull(x.getStreamFactory());
		assertNotNull(x.getPeriods());
		assertNotNull(x.getByteUtils());
	}
	
	@Test
	public void testCreateServer() {
		IService actual = service.createServer("localhost", 9698, new Object());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(JettyServerStarter.class)));
		JettyServerStarter x = (JettyServerStarter) actual;
		Server s = x.getServer();
		assertNotNull(s);
	}

	@Test
	public void testBuild3() throws Exception {
		Object componentStub = new Object();
		Capture<String> cap1 = newCapture(), cap2 = newCapture();
		configStub = new BacknodeConfig() {
			@Override
			public void load(String default_config_file, String config_file) {
				cap1.setValue(default_config_file);
				cap2.setValue(config_file);
			}
		};
		configStub.getProperties().put("caelum.backnode.rest.http.host", "bambr1");
		configStub.getProperties().put("caelum.backnode.rest.http.port", "7281");
		expect(mockedService.createConfig()).andReturn(configStub);
		expect(mockedService.createComponent(caelumMock)).andReturn(componentStub);
		expect(mockedService.createServer("bambr1", 7281, componentStub)).andReturn(serviceMock);
		control.replay();
		replay(mockedService);
		
		IService actual = mockedService.build("foo.props", "bar.props", caelumMock);
		
		verify(mockedService);
		control.verify();
		assertSame(serviceMock, actual);
	}

}
