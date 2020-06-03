package ru.prolib.caelum.restapi;

import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.IKafkaStreamsRegistry;

public class RestService {
	static final Logger logger = LoggerFactory.getLogger(RestService.class);
	private final HostInfo hostInfo;
	private final RestEndpoints endpoints;
	private Server jettyServer;
	
	public RestService(HostInfo host_info) {
		this.hostInfo = host_info;
		this.endpoints = new RestEndpoints(hostInfo);
	}
	
	public IKafkaStreamsRegistry getKafkaStreamsRegistry() {
		return endpoints;
	}
	
	public synchronized void start() throws Exception {
		if ( jettyServer != null ) {
			throw new IllegalStateException("Server already started");
		}
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		
		jettyServer = new Server(hostInfo.port());
		try {
			jettyServer.setHandler(context);
			
			ResourceConfig rc = new ResourceConfig();
			rc.register(endpoints);
			rc.register(JacksonFeature.class);
			
			context.addServlet(new ServletHolder(new ServletContainer(rc)), "/*");
			
			jettyServer.start();
			logger.info("Server started");
		} catch ( Throwable t ) {
			jettyServer = null;
			throw t;
		}
	}
	
	public synchronized void stop() throws Exception {
		if ( jettyServer != null ) {
			jettyServer.stop();
			jettyServer = null;
			logger.info("Server stopped");
		}
	}
	
}
