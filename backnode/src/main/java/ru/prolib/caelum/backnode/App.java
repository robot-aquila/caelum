package ru.prolib.caelum.backnode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.aggregator.app.ItemAggregatorBuilder;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		CompositeService services = new CompositeService();
		ItemAggregatorBuilder item_aggregator_builder = new ItemAggregatorBuilder();
		ICaelum caelum = new CaelumBuilder()
				.withAggregatorService(item_aggregator_builder.getAggregatorService())
				.build();
		ItemAggregatorConfig aggr_conf = new ItemAggregatorConfig();
		aggr_conf.getProperties().put(ItemAggregatorConfig.BOOTSTRAP_SERVERS, "192.168.99.100:32769");
		aggr_conf.getProperties().put(ItemAggregatorConfig.AGGREGATION_PERIOD, "M1");
		services.register(item_aggregator_builder.build(aggr_conf));
		services.register(new JettyServerBuilder()
			.withHost("192.168.1.22")
			.withPort(60606)
			.withComponent(new NodeService(caelum, new Freemarker(), new JsonFactory(), Periods.getInstance()))
			.build());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> services.stop()));
		services.start();
	}

}
