package ru.prolib.caelum.backnode.rest.jetty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.servlet.Servlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.ServletMapping;

public class JettyServerBuilder2 {
	private String host;
	private Integer port;
	private final List<ServletMapping> servlets;
	
	public JettyServerBuilder2() {
		servlets = new ArrayList<>();
	}
	
	public JettyServerBuilder2 withHost(String host) {
		this.host = host;
		return this;
	}
	
	public JettyServerBuilder2 withPort(int port) {
		this.port = port;
		return this;
	}
	
	public JettyServerBuilder2 addServlet(ServletMapping spec) {
		servlets.add(spec);
		return this;
	}
	
	public JettyServerBuilder2 addServlet(Servlet servlet, String pathSpec) {
		return addServlet(new ServletMapping(servlet, pathSpec));
	}
	
	protected IService createServer(String host, int port, Collection<ServletMapping> servlets) {
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		Server server = new Server(new InetSocketAddress(host, port));
		server.setHandler(context);
		servlets.stream().forEach(s -> context.addServlet(new ServletHolder(s.getServlet()), s.getPathSpec()));
		return new JettyServerStarter(server);
	}
	
	public IService build() {
		if ( host == null ) {
			throw new IllegalStateException("Server host was not defined");
		}
		if ( port == null ) {
			throw new IllegalStateException("Server port was not defined");
		}
		if ( servlets.size() == 0 ) {
			throw new IllegalStateException("No servlets were defined");
		}
		return createServer(host, port, servlets);
	}
	
}
