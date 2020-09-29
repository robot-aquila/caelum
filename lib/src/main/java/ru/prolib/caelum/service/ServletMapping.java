package ru.prolib.caelum.service;

import javax.servlet.Servlet;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class ServletMapping {
	private final Servlet servlet;
	private final String pathSpec;
	
	public ServletMapping(Servlet servlet, String pathSpec) {
		this.servlet = servlet;
		this.pathSpec = pathSpec;
	}
	
	public Servlet getServlet() {
		return servlet;
	}
	
	public String getPathSpec() {
		return pathSpec;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ServletMapping.class ) {
			return false;
		}
		ServletMapping o = (ServletMapping) other;
		return new EqualsBuilder()
				.append(o.servlet, servlet)
				.append(o.pathSpec, pathSpec)
				.build();
	}

}
