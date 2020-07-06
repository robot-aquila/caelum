package ru.prolib.caelum.backnode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.aggregator.app.ItemAggregatorBuilder;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) throws Exception {
		String default_config_file = AppConfig.DEFAULT_CONFIG_FILE, config_file = args.length > 0 ? args[0] : null;
		AppConfig config = new AppConfig();
		config.load(config_file);
		
		CompositeService services = new CompositeService();
		ItemAggregatorBuilder item_aggregator_builder = new ItemAggregatorBuilder();
		ICaelum caelum = new CaelumBuilder()
			.withAggregatorService(item_aggregator_builder.getAggregatorService())
			.withItemDatabaseService(new ItemDatabaseServiceBuilder().build(default_config_file, config_file, services))
			.withSymbolService(new SymbolServiceBuilder().build(default_config_file, config_file, services))
			.build();
		services.register(item_aggregator_builder.build(config.getItemAggregatorConfig()));
		services.register(new JettyServerBuilder()
			.withHost("127.0.0.1")
			.withPort(60606)
			.withComponent(new NodeService(caelum, new Freemarker(), new JsonFactory(), Periods.getInstance()))
			.build());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> services.stop()));
		services.start();
	}

}
