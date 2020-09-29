package ru.prolib.caelum.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.Servlet;

public class ServletRegistry {
	private final List<ServletMapping> servlets;
	
	public ServletRegistry(List<ServletMapping> servlets) {
		this.servlets = servlets;
	}
	
	public ServletRegistry() {
		this(new ArrayList<>());
	}
	
	public ServletRegistry registerServlet(ServletMapping servlet) {
		servlets.add(servlet);
		return this;
	}
	
	public ServletRegistry registerServlet(Servlet servlet, String pathSpec) {
		return registerServlet(new ServletMapping(servlet, pathSpec));
	}
	
	public Collection<ServletMapping> getServlets() {
		return servlets;
	}
	
}
