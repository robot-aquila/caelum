package ru.prolib.caelum.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.Servlet;

public class ServletRegistry implements IServletRegistry {
	private final List<ServletMapping> servlets;
	
	public ServletRegistry(List<ServletMapping> servlets) {
		this.servlets = servlets;
	}
	
	public ServletRegistry() {
		this(new ArrayList<>());
	}
	
	@Override
	public ServletRegistry registerServlet(ServletMapping servlet) {
		servlets.add(servlet);
		return this;
	}
	
	@Override
	public ServletRegistry registerServlet(Servlet servlet, String pathSpec) {
		return registerServlet(new ServletMapping(servlet, pathSpec));
	}
	
	public Collection<ServletMapping> getServlets() {
		return servlets;
	}
	
}
