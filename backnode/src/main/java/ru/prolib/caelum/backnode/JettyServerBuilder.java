package ru.prolib.caelum.backnode;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
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
			throw new NullPointerException("Server host was not defined");
		}
		if ( port == null ) {
			throw new NullPointerException("Server port was not defined");
		}
		if ( component == null ) {
			throw new NullPointerException("Component was not defined");
		}
		
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		
		Server server = new Server(new InetSocketAddress(host, port));
		server.setHandler(context);
			
		ResourceConfig rc = new ResourceConfig();
		rc.register(component);
		rc.register(WebApplicationExceptionMapper.class);
		rc.register(ThrowableMapper.class);
		rc.register(JacksonFeature.class);
			
		context.addServlet(new ServletHolder(new ServletContainer(rc)), "/*");
		return new JettyServerStarter(server);
	}
	
}
