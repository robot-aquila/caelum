package ru.prolib.caelum.backnode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import ru.prolib.caelum.lib.IService;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) throws Exception {
		logger.info("Starting up...");
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
		
		IService service = new BacknodeBuilder().build(args.length > 0 ? args[0] : null);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> service.stop()));
		service.start();
	}

}
