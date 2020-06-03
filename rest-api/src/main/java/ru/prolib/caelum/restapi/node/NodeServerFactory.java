package ru.prolib.caelum.restapi.node;

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
import ru.prolib.caelum.restapi.exception.ThrowableMapper;
import ru.prolib.caelum.restapi.exception.WebApplicationExceptionMapper;

public class NodeServerFactory {
	static final Logger logger = LoggerFactory.getLogger(NodeServerFactory.class);
	private final HostInfo hostInfo;
	private final NodeService endpoints;
	private Server jettyServer;
	
	public NodeServerFactory(HostInfo host_info) {
		this.hostInfo = host_info;
		this.endpoints = new NodeService(hostInfo);
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
			rc.register(WebApplicationExceptionMapper.class);
			rc.register(ThrowableMapper.class);
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
