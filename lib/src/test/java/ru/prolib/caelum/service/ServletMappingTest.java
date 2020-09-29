package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import javax.servlet.Servlet;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

public class ServletMappingTest {
	IMocksControl control;
	Servlet servletMock1, servletMock2;
	ServletMapping service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		servletMock1 = control.createMock(Servlet.class);
		servletMock2 = control.createMock(Servlet.class);
		service = new ServletMapping(servletMock1, "/foo");
	}
	
	@Test
	public void testGetters() {
		assertSame(servletMock1, service.getServlet());
		assertEquals("/foo", service.getPathSpec());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ServletMapping(servletMock1, "/foo")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new ServletMapping(servletMock2, "/foo")));
		assertFalse(service.equals(new ServletMapping(servletMock1, "/bar")));
		assertFalse(service.equals(new ServletMapping(servletMock2, "/bar")));
	}
	
}
