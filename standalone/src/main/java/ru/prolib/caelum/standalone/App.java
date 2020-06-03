package ru.prolib.caelum.standalone;

import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.TradeAggregator;
import ru.prolib.caelum.aggregator.TradeAggregatorConfig;
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
		
		TradeAggregatorConfig aggr_conf = new TradeAggregatorConfig();
		aggr_conf.getProperties().put(TradeAggregatorConfig.BOOTSTRAP_SERVERS, "192.168.99.100:32768");
		aggr_conf.getProperties().put(TradeAggregatorConfig.AGGREGATION_PERIOD, "M1");
		TradeAggregator aggr = new TradeAggregator();
		aggr.start(aggr_conf, rest.getKafkaStreamsRegistry());
	}

}
