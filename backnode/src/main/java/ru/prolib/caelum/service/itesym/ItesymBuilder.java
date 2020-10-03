package ru.prolib.caelum.service.itesym;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.IExtension;
import ru.prolib.caelum.service.IExtensionBuilder;

public class ItesymBuilder implements IExtensionBuilder {
	
	protected KafkaConsumer<String, byte[]> createConsumer(Properties props) {
		return new KafkaConsumer<>(props, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
	}
	
	protected Thread createThread(String name, Runnable r) {
		return new Thread(r, name);
	}

	@Override
	public IExtension build(IBuildingContext context) throws IOException {
		GeneralConfig config = context.getConfig();
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getItesymKafkaGroupId());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		KafkaConsumer<String, byte[]> consumer = createConsumer(props);
		final String group_id = config.getItesymKafkaGroupId();
		AtomicReference<Thread> thread_ref = new AtomicReference<>();
		Itesym extension = new Itesym(group_id,
				consumer,
				config.getItemsTopicName(),
				context.getCaelum(),
				config.getKafkaPollTimeout(),
				config.getShutdownTimeout(),
				thread_ref);
		thread_ref.set(createThread(group_id, extension));
		context.registerService(extension);
		return extension;
	}

}
