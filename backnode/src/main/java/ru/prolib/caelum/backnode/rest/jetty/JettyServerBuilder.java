package ru.prolib.caelum.backnode.rest.jetty;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.servlet.Servlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import ru.prolib.caelum.backnode.BacknodeConfig;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.BuildingContext;
import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ExtensionStub;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.IExtension;
import ru.prolib.caelum.service.IExtensionBuilder;
import ru.prolib.caelum.service.ServletRegistry;

public class JettyServerBuilder implements IExtensionBuilder {
	
	protected BacknodeConfig createConfig() {
		return new BacknodeConfig();
	}
	
	protected ServletContextHandler createContextHandler() {
		return new ServletContextHandler(ServletContextHandler.SESSIONS);
	}
	
	protected Server createJettyServer(String host, int port) {
		return new Server(new InetSocketAddress(host, port));
	}
	
	protected ServletHolder createServletHolder(Servlet servlet) {
		return new ServletHolder(servlet);
	}
	
	protected IService createServer(String host, int port, ServletRegistry servlets) {
		ServletContextHandler context = createContextHandler();
		context.setContextPath("/");
		Server server = createJettyServer(host, port);
		server.setHandler(context);
		servlets.getServlets().stream()
			.forEach(s -> context.addServlet(createServletHolder(s.getServlet()), s.getPathSpec()));
		return new JettyServerStarter(server);
	}
	
	@Override
	public IExtension build(IBuildingContext context) throws IOException {
		BacknodeConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		ServletRegistry servlets = (ServletRegistry) ((BuildingContext) context).getServlets();
		context.registerService(createServer(config.getRestHttpHost(), config.getRestHttpPort(), servlets));
		return new ExtensionStub(new ExtensionStatus("HTTP", ExtensionState.RUNNING, null));
	}
	
}
