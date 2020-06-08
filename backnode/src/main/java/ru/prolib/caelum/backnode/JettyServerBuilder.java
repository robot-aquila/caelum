package ru.prolib.caelum.backnode;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import ru.prolib.caelum.core.IService;

public class JettyServerBuilder {
	private String host;
	private Integer port;
	private Object component;
	
	public JettyServerBuilder withHost(String host) {
		this.host = host;
		return this;
	}
	
	public JettyServerBuilder withPort(int port) {
		this.port = port;
		return this;
	}
	
	public JettyServerBuilder withComponent(Object component) {
		this.component = component;
		return this;
	}
	
	public IService build() {
		if ( host == null ) {
			throw new IllegalStateException("Server host was not defined");
		}
		if ( port == null ) {
			throw new IllegalStateException("Server port was not defined");
		}
		if ( component == null ) {
			throw new IllegalStateException("Component was not defined");
		}
		
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		
		Server server = new Server(new InetSocketAddress(host, port));
		server.setHandler(context);
			
		CommonResourceConfig rc = new CommonResourceConfig();
		rc.register(component);
			
		context.addServlet(new ServletHolder(new ServletContainer(rc)), "/*");
		return new JettyServerStarter(server);
	}
	
}
