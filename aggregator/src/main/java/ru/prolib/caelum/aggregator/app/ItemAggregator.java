package ru.prolib.caelum.aggregator.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.IService;

public class ItemAggregator {
	static final Logger logger = LoggerFactory.getLogger(ItemAggregator.class);

	public static void main(String[] args) throws Exception {
		ItemAggregatorBuilder builder = new ItemAggregatorBuilder();
		IService service = builder.build(args.length > 0 ? args[0] : null);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> service.stop()));
		service.start();
	}

}
