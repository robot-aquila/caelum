package ru.prolib.caelum.itemdb.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import ru.prolib.caelum.core.CaelumSerdes;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.itemdb.ItemDatabaseConfig;

public class ItemDatabaseService implements IItemDatabaseService {
	private final ItemDatabaseConfig config;
	
	public ItemDatabaseService(ItemDatabaseConfig config) {
		this.config = config;
	}
	
	private KafkaConsumer<String, Item> createConsumer() {
		return new KafkaConsumer<>(config.getKafkaProperties(),
			CaelumSerdes.keySerde().deserializer(), CaelumSerdes.itemSerde().deserializer());
	}

	@Override
	public IItemDataIterator fetch(ItemDataRequest request) {
		KafkaConsumer<String, Item> consumer = createConsumer();
		List<PartitionInfo> partitions = consumer.partitionsFor(config.getSourceTopic());
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
