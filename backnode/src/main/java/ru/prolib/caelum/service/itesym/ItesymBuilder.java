package ru.prolib.caelum.service.itesym;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.IExtension;
import ru.prolib.caelum.service.IExtensionBuilder;

public class ItesymBuilder implements IExtensionBuilder {
	
	protected ItesymConfig createConfig() {
		return new ItesymConfig();
	}
	
	protected KafkaConsumer<String, byte[]> createConsumer(Properties props) {
		return new KafkaConsumer<>(props, Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
	}
	
	protected Thread createThread(String name, Runnable r) {
		return new Thread(r, name);
	}

	@Override
	public IExtension build(IBuildingContext context) throws IOException {
		ItesymConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		KafkaConsumer<String, byte[]> consumer = createConsumer(config.getKafkaProperties());
		final String group_id = config.getGroupId();
		AtomicReference<Thread> thread_ref = new AtomicReference<>();
		Itesym extension = new Itesym(group_id,
				consumer,
				config.getSourceTopic(),
				context.getCaelum(),
				config.getPollTimeout(),
				config.getShutdownTimeout(),
				thread_ref);
		thread_ref.set(createThread(group_id, extension));
		context.registerService(extension);
		return extension;
	}

}
