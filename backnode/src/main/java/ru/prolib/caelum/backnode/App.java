package ru.prolib.caelum.backnode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.IService;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) throws Exception {
		logger.info("Starting up...");
		IService service = new BacknodeBuilder().build(args.length > 0 ? args[0] : null);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> service.stop()));
		service.start();
	}

}
