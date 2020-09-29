package ru.prolib.caelum.service.aggregator.kafka;

import java.util.concurrent.locks.Lock;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.service.AggregatorType;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.service.aggregator.kafka.utils.RecoverableStreamsService;
import ru.prolib.caelum.service.aggregator.kafka.utils.RecoverableStreamsServiceStarter;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorBuilder {
	
	public static class Objects {
		private KafkaUtils utils;
		private KafkaAggregatorTopologyBuilder topologyBuilder;
		private KafkaAggregatorConfig config;
		private KafkaStreamsRegistry streamsRegistry;
		private CompositeService services;
		private Lock cleanUpMutex;
		
		public Objects setUtils(KafkaUtils utils) {
			this.utils = utils;
			return this;
		}
		
		public Objects setTopologyBuilder(KafkaAggregatorTopologyBuilder builder) {
			this.topologyBuilder = builder;
			return this;
		}
		
		public Objects setConfig(KafkaAggregatorConfig config) {
			this.config = config;
			return this;
		}
		
		public Objects setStreamsRegistry(KafkaStreamsRegistry registry) {
			this.streamsRegistry = registry;
			return this;
		}
		
		public Objects setServices(CompositeService services) {
			this.services = services;
			return this;
		}
		
		public Objects setCleanUpMutex(Lock mutex) {
			this.cleanUpMutex = mutex;
			return this;
		}
		
		public KafkaUtils getUtils() {
			if ( utils == null ) {
				throw new IllegalStateException("Kafka utils was not defined");
			}
			return utils;
		}
		
		public KafkaAggregatorTopologyBuilder getTopologyBuilder() {
			if ( topologyBuilder == null ) {
				throw new IllegalStateException("Topology builder was not defined");
			}
			return topologyBuilder;
		}
		
		public KafkaAggregatorConfig getConfig() {
			if ( config == null ) {
				throw new IllegalStateException("Configuration was not defined");
			}
			return config;
		}
		
		public KafkaStreamsRegistry getStreamsRegistry() {
			if ( streamsRegistry == null ) {
				throw new IllegalStateException("Streams registry was not defined");
			}
			return streamsRegistry;
		}
		
		public CompositeService getServices() {
			if ( services == null ) {
				throw new IllegalStateException("Services was not defined");
			}
			return services;
		}
		
		public Lock getCleanUpMutex() {
			if ( cleanUpMutex == null ) {
				throw new IllegalStateException("CleanUp mutex was not defined");
			}
			return cleanUpMutex;
		}
		
	}
	
	private final Objects objects;
	
	public KafkaAggregatorBuilder(Objects objects) {
		this.objects = objects;
	}
	
	public KafkaAggregatorBuilder() {
		this(new Objects());
	}
	
	public KafkaAggregatorBuilder withUtils(KafkaUtils utils) {
		objects.setUtils(utils);
		return this;
	}
	
	public KafkaAggregatorBuilder withTopologyBuilder(KafkaAggregatorTopologyBuilder builder) {
		objects.setTopologyBuilder(builder);
		return this;
	}
	
	public KafkaAggregatorBuilder withConfig(KafkaAggregatorConfig config) {
		objects.setConfig(config);
		return this;
	}
	
	public KafkaAggregatorBuilder withStreamsRegistry(KafkaStreamsRegistry registry) {
		objects.setStreamsRegistry(registry);
		return this;
	}
	
	public KafkaAggregatorBuilder withServices(CompositeService services) {
		objects.setServices(services);
		return this;
	}
	
	public KafkaAggregatorBuilder withCleanUpMutex(Lock mutex) {
		objects.setCleanUpMutex(mutex);
		return this;
	}
	
	protected Thread createThread(String name, Runnable runnable) {
		return new Thread(runnable, name);
	}
	
	protected RecoverableStreamsService createStreamsService(KafkaAggregatorDescr descr) {
		return new RecoverableStreamsService(
			new KafkaStreamsController(descr,
				objects.getTopologyBuilder(),
				objects.getConfig(),
				objects.getStreamsRegistry(),
				objects.getCleanUpMutex(),
				objects.getUtils()),
			objects.getConfig().getMaxErrors());	
	}
	
	protected IAggregator createAggregator(KafkaAggregatorDescr descr, IRecoverableStreamsService streamsService) {
		return new KafkaAggregator(descr, objects.getConfig(), streamsService,
				objects.getUtils(), objects.getStreamsRegistry());
	}
	
	public IAggregator build() {
		KafkaAggregatorConfig config = objects.getConfig();
		KafkaAggregatorDescr descr = new KafkaAggregatorDescr(AggregatorType.ITEM,
				config.getAggregationInterval(),
				config.getSourceTopic(),
				config.getTargetTopic(),
				config.getStoreName());
		RecoverableStreamsService streamsService = createStreamsService(descr);
		objects.getServices().register(new RecoverableStreamsServiceStarter(
				createThread(config.getApplicationId() + "-thread", streamsService),
				streamsService,
				config.getDefaultTimeout()));
		return createAggregator(descr, streamsService);
	}

}
