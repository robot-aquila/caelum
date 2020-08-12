package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.rest.IRestServiceBuilder;
import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;

public class BacknodeBuilderTest {
	IMocksControl control;
	CompositeService servicesMock;
	CaelumBuilder caelumBuilderMock;
	ICaelum caelumMock;
	IRestServiceBuilder restServiceBuilderMock;
	IService restServiceMock;
	BacknodeBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		servicesMock = control.createMock(CompositeService.class);
		caelumBuilderMock = control.createMock(CaelumBuilder.class);
		caelumMock = control.createMock(ICaelum.class);
		restServiceBuilderMock = control.createMock(IRestServiceBuilder.class);
		restServiceMock = control.createMock(IService.class);
		service = new BacknodeBuilder();
		mockedService = partialMockBuilder(BacknodeBuilder.class)
				.addMockedMethod("createServices")
				.addMockedMethod("createCaelumBuilder")
				.addMockedMethod("createRestServerBuilder")
				.createMock();
	}
	
	@Test
	public void testCreateServices() {
		CompositeService actual = service.createServices();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateCaelumBuilder() {
		CaelumBuilder actual = service.createCaelumBuilder();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateRestServerBuilder() {
		IRestServiceBuilder actual = service.createRestServerBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(JettyServerBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		expect(mockedService.createServices()).andReturn(servicesMock);
		expect(mockedService.createCaelumBuilder()).andReturn(caelumBuilderMock);
		expect(caelumBuilderMock.build("app.backnode.properties", "bar.props", servicesMock)).andReturn(caelumMock);
		expect(mockedService.createRestServerBuilder()).andReturn(restServiceBuilderMock);
		expect(restServiceBuilderMock.build("app.backnode.properties", "bar.props", caelumMock))
			.andReturn(restServiceMock);
		expect(servicesMock.register(restServiceMock)).andReturn(servicesMock);
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
