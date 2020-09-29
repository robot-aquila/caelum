package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.servlet.Servlet;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

public class ServletRegistryTest {
	IMocksControl control;
	Servlet servletMock1, servletMock2, servletMock3;
	List<ServletMapping> servlets;
	ServletRegistry service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		servletMock1 = control.createMock(Servlet.class);
		servletMock2 = control.createMock(Servlet.class);
		servletMock3 = control.createMock(Servlet.class);
		servlets = new ArrayList<>();
		service = new ServletRegistry();
	}
	
	@Test
	public void testRegisterServlet1() {
		assertSame(service, service.registerServlet(new ServletMapping(servletMock1, "/foo/*")));
		assertSame(service, service.registerServlet(new ServletMapping(servletMock2, "/bar/*")));
		assertSame(service, service.registerServlet(new ServletMapping(servletMock3, "/buz/*")));

		assertEquals(Arrays.asList(
				new ServletMapping(servletMock1, "/foo/*"),
				new ServletMapping(servletMock2, "/bar/*"),
				new ServletMapping(servletMock3, "/buz/*")
			), service.getServlets());
	}
	
	@Test
	public void testRegisterServlet2() {
		assertSame(service, service.registerServlet(servletMock1, "/gap/*"));
		assertSame(service, service.registerServlet(servletMock2, "/pop/*"));
		assertSame(service, service.registerServlet(servletMock3, "/dub/*"));
		
		assertEquals(Arrays.asList(
				new ServletMapping(servletMock1, "/gap/*"),
				new ServletMapping(servletMock2, "/pop/*"),
				new ServletMapping(servletMock3, "/dub/*")
			), service.getServlets());
	}
	
	@Test
	public void testGetServlets() {
		service.registerServlet(servletMock1, "/gap/*")
			.registerServlet(servletMock2, "/pop/*")
			.registerServlet(servletMock3, "/dub/*");
		
		Collection<ServletMapping> expected = Arrays.asList(
				new ServletMapping(servletMock1, "/gap/*"),
				new ServletMapping(servletMock2, "/pop/*"),
				new ServletMapping(servletMock3, "/dub/*")
			);
		assertEquals(expected, service.getServlets());
	}

}
