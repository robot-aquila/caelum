package ru.prolib.caelum.backnode.rest.jetty;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

import ru.prolib.caelum.backnode.BacknodeConfig;
import ru.prolib.caelum.backnode.CommonResourceConfig;
import ru.prolib.caelum.backnode.NodeService;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.backnode.mvc.StreamFactory;
import ru.prolib.caelum.backnode.rest.IRestServiceBuilder;
import ru.prolib.caelum.core.ByteUtils;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.service.ICaelum;

public class JettyServerBuilder implements IRestServiceBuilder {
	private String host;
	private Integer port;
	private Object component;
	
	protected BacknodeConfig createConfig() {
		return new BacknodeConfig();
	}
	
	protected Object createComponent(ICaelum caelum) {
		return new NodeService(caelum, new Freemarker(), new StreamFactory(),
				Periods.getInstance(), ByteUtils.getInstance());
	}
	
	protected IService createServer(String host, int port, Object component) {
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		
		Server server = new Server(new InetSocketAddress(host, port));
		server.setHandler(context);
			
		CommonResourceConfig rc = new CommonResourceConfig();
		rc.register(component);
			
		context.addServlet(new ServletHolder(new ServletContainer(rc)), "/*");
		return new JettyServerStarter(server);
	}
	
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
		return createServer(host, port, component);
	}
	
	@Override
	public IService build(String default_config_file, String config_file, ICaelum caelum) throws IOException {
		BacknodeConfig config = createConfig();
		config.load(default_config_file, config_file);
		return createServer(config.getRestHttpHost(), config.getRestHttpPort(), createComponent(caelum));
	}
		
}
