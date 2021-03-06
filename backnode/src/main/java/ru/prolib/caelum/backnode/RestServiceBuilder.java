package ru.prolib.caelum.backnode;

import java.io.IOException;

import javax.servlet.Servlet;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import ru.prolib.caelum.backnode.mvc.StreamFactory;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ExtensionStub;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.IExtension;
import ru.prolib.caelum.service.IExtensionBuilder;
import ru.prolib.caelum.service.Mode;

public class RestServiceBuilder implements IExtensionBuilder {
	
	protected Servlet createConsoleStaticFilesServlet() {
		return new StaticResourceServlet();
	}
	
	protected Object createRestServiceComponent(ICaelum caelum, boolean testMode) {
		return new RestService(caelum, new StreamFactory(), new Intervals(), testMode);
	}
	
	protected Servlet createServletContainer(ResourceConfig rc) {
		return new ServletContainer(rc);
	}
	
	protected Servlet createRestServiceServlet(ICaelum caelum, boolean testMode) {
		CommonResourceConfig rc = new CommonResourceConfig();
		rc.register(createRestServiceComponent(caelum, testMode));
		return createServletContainer(rc);
	}
	
	@Override
	public IExtension build(IBuildingContext context) throws IOException {
		GeneralConfig config = context.getConfig();
		context.registerServlet(createConsoleStaticFilesServlet(), "/console/*");
		// TODO: Remove WS to its own extension
		//context.registerServlet(new WebSocketServletImpl(new TestCreator()), "/ws");
		context.registerServlet(createRestServiceServlet(context.getCaelum(), config.getMode() == Mode.TEST), "/*");
		return new ExtensionStub(new ExtensionStatus(ExtensionState.RUNNING, null));
	}

}
