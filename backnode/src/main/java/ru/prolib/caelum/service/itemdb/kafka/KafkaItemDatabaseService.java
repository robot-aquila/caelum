package ru.prolib.caelum.service.itemdb.kafka;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.lib.kafka.KafkaItem;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.ItemDataRequest;
import ru.prolib.caelum.service.ItemDataRequestContinue;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaItemDatabaseService implements IItemDatabaseService {
	static final Logger logger = LoggerFactory.getLogger(KafkaItemDatabaseService.class);
	private final GeneralConfig config;
	private final KafkaProducer<String, KafkaItem> producer;
	private final KafkaUtils utils;
	private final Clock clock;
	
	KafkaItemDatabaseService(GeneralConfig config,
			KafkaProducer<String, KafkaItem> producer,
			KafkaUtils utils,
			Clock clock)
	{
		this.config = config;
		this.producer = producer;
		this.utils = utils;
		this.clock = clock;
	}
	
	public KafkaItemDatabaseService(GeneralConfig config, KafkaProducer<String, KafkaItem> producer) {
		this(config, producer, KafkaUtils.getInstance(), Clock.systemUTC());
	}
	
	public GeneralConfig getConfig() {
		return config;
	}
	
	protected KafkaConsumer<String, KafkaItem> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		return utils.createConsumer(props);
	}
	
	protected AdminClient createAdmin() {
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
		return utils.createAdmin(props);
	}
	
	private int getLimit(Integer requested_limit) {
		int default_limit = config.getMaxItemsLimit();
		return requested_limit == null ? default_limit : Math.min(requested_limit, default_limit); 		
	}
	
	private int getLimit(ItemDataRequest request) {
		return getLimit(request.getLimit());
	}

	private int getLimit(ItemDataRequestContinue request) {
		return getLimit(request.getLimit());
	}
	
	public KafkaProducer<String, KafkaItem> getProducer() {
		return producer;
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}
	
	public Clock getClock() {
		return clock;
	}

	@Override
	public IItemIterator fetch(ItemDataRequest req) {
		KafkaConsumer<String, KafkaItem> consumer = createConsumer();
		KafkaItemInfo item_info = utils.getItemInfo(consumer, config.getItemsTopicName(), req.getSymbol());
		if ( item_info.hasData() ) {
			TopicPartition tp = item_info.toTopicPartition();
			consumer.assign(Arrays.asList(tp));
			consumer.seek(tp, utils.getOffset(consumer, tp, req.getFrom(), item_info.getStartOffset()));
			return utils.createIterator(consumer, item_info, getLimit(req), req.getFrom(), req.getTo(), clock);
		} else {
			return utils.createIteratorStub(consumer, item_info, getLimit(req), req.getFrom(), req.getTo());
		}
	}

	@Override
	public IItemIterator fetch(ItemDataRequestContinue req) {
		KafkaConsumer<String, KafkaItem> consumer = createConsumer();
		KafkaItemInfo item_info = utils.getItemInfo(consumer, config.getItemsTopicName(), req.getSymbol());
		if ( item_info.hasData() && req.getOffset() < item_info.getEndOffset() ) {
			TopicPartition tp = item_info.toTopicPartition();
			consumer.assign(Arrays.asList(tp));
			consumer.seek(tp, req.getOffset());
			return utils.createIterator(consumer, item_info, getLimit(req), null, req.getTo(), clock);
		} else {
			return utils.createIteratorStub(consumer, item_info, getLimit(req), null, req.getTo());
		}
	}
	
	protected void appendRecordOffsetInfo(StringBuilder sb, String typeMsg, Future<RecordMetadata> f) {
		if ( f == null ) {
			sb.append("No ").append(typeMsg).append(" record info provided.");
			return;
		}
		try {
			RecordMetadata md = f.get(1, TimeUnit.SECONDS);
			if ( md.hasOffset() == false ) {
				sb.append(typeMsg).append(" offset not available.");
			} else {
				sb.append(typeMsg).append(" offset: ").append(md.offset()).append(".");
			}
		} catch ( InterruptedException | TimeoutException | ExecutionException e ) {
			logger.error("Error fetching {} record MD: ", typeMsg, e);
		}
	}
	
	protected void dumpTransactionInfo(Future<RecordMetadata> first,
			Future<RecordMetadata> last,
			long start_time,
			int num_items)
	{
		//StringBuilder sb = new StringBuilder();
		//sb.append("Total ").append(num_items).append(" items committed in ")
		//	.append(clock.millis() - start_time).append("ms.");
		
		//sb.append(" ");
		//appendRecordOffsetInfo(sb, "first", first);
		//sb.append(" ");
		//appendRecordOffsetInfo(sb, "last", last);
		//logger.debug(sb.toString());
	}
	
	@Override
	public void registerItem(Collection<IItem> items) {
		final String topic = config.getItemsTopicName();
		final long start_time = clock.millis();
		Future<RecordMetadata> first = null, last = null;
		try {
			producer.beginTransaction();
			for ( IItem item : items ) {
				last = producer.send(new ProducerRecord<>(topic, null, item.getTime(), item.getSymbol(),
					new KafkaItem(item.getValue(), item.getDecimals(), item.getVolume(), item.getVolumeDecimals(),
						item.getType())));
				if ( first == null ) first = last;
			}
			producer.commitTransaction();
			dumpTransactionInfo(first, last, start_time, items.size());
		} catch ( ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e ) {
			producer.close();
		} catch ( KafkaException e ) {
			producer.abortTransaction();
		}
	}

	@Override
	public void registerItem(IItem item) {
		registerItem(Arrays.asList(item));
	}

	@Override
	public void clear(boolean global) {
		if ( global ) {
			try ( AdminClient admin = createAdmin() ) {
				utils.deleteRecords(admin, config.getItemsTopicName(), config.getDefaultTimeout());
			}
		}
	}

}
