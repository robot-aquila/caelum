package ru.prolib.caelum.itemdb.kafka;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.itemdb.ItemDatabaseConfig;

public class ItemDatabaseService implements IItemDatabaseService {
	private final ItemDatabaseConfig config;
	private final KafkaUtils utils;
	
	ItemDatabaseService(ItemDatabaseConfig config, KafkaUtils utils) {
		this.config = config;
		this.utils = utils;
	}
	
	public ItemDatabaseService(ItemDatabaseConfig config) {
		this(config, KafkaUtils.getInstance());
	}
	
	private KafkaConsumer<String, Item> createConsumer() {
		return new KafkaConsumer<>(config.getKafkaProperties(),
			CaelumSerdes.keySerde().deserializer(), CaelumSerdes.itemSerde().deserializer());
	}

	@Override
	public IItemDataIterator fetch(ItemDataRequest request) {
		KafkaConsumer<String, Item> consumer = createConsumer();
		ItemInfo item_info = utils.getItemInfo(consumer, config.getSourceTopic(), request.getSymbol());
		if ( item_info.hasData() ) {
			consumer.assign(Arrays.asList(item_info.toTopicPartition()));
			// TODO: 
		} else {
			return new ItemDataIterator(consumer, new IteratorStub<>(), item_info, request.getLimit());
		}
		
		
		
		
		// choose partition
		// assign
		// get beginning offset
		// get end offset
		// choose position
		// poll until end offset or limit reached
		// 
		
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IItemDataIterator fetch(ItemDataRequestContinue request) {
		// TODO Auto-generated method stub
		return null;
	}

}
