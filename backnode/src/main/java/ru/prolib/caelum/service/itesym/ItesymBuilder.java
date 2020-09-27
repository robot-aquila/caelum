package ru.prolib.caelum.service.itesym;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.service.ICaelum;
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
	public IExtension build(String default_config_file, String config_file, CompositeService services, ICaelum caelum)
			throws IOException
	{
		ItesymConfig config = createConfig();
		config.load(default_config_file, config_file);
		KafkaConsumer<String, byte[]> consumer = createConsumer(config.getKafkaProperties());
		final String group_id = config.getGroupId();
		AtomicReference<Thread> thread_ref = new AtomicReference<>();
		Itesym extension = new Itesym(group_id,
				consumer,
				config.getSourceTopic(),
				caelum,
				config.getPollTimeout(),
				config.getShutdownTimeout(),
				thread_ref);
		thread_ref.set(createThread(group_id, extension));
		services.register(extension);
		return extension;
	}

}
