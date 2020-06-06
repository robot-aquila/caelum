package ru.prolib.caelum.backnode;

import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.aggregator.app.ItemAggregator;
import ru.prolib.caelum.restapi.node.NodeServerFactory;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		HostInfo host_info = new HostInfo("192.168.1.22", 60606);
		NodeServerFactory rest = new NodeServerFactory(host_info);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				rest.stop();
			} catch ( Exception e ) {
				logger.error("Unexpected exception: ", e);
			}
		}));
		rest.start();
		
		ItemAggregatorConfig aggr_conf = new ItemAggregatorConfig();
		aggr_conf.getProperties().put(ItemAggregatorConfig.BOOTSTRAP_SERVERS, "192.168.99.100:32769");
		aggr_conf.getProperties().put(ItemAggregatorConfig.AGGREGATION_PERIOD, "M1");
		ItemAggregator aggr = new ItemAggregator();
		aggr.start(aggr_conf, rest.getKafkaStreamsRegistry());
	}

}
