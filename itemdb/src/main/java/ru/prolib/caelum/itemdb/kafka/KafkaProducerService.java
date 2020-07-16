package ru.prolib.caelum.itemdb.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;

import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.ServiceException;

public class KafkaProducerService implements IService {
	private final KafkaProducer<?, ?> producer;
	
	public KafkaProducerService(KafkaProducer<?, ?> producer) {
		this.producer = producer;
	}

	@Override
	public void start() throws ServiceException {
		
	}

	@Override
	public void stop() throws ServiceException {
		producer.close();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(90017625, 43)
				.append(producer)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaProducerService.class ) {
			return false;
		}
		KafkaProducerService o = (KafkaProducerService) other;
		return new EqualsBuilder()
				.append(o.producer, producer)
				.build();
	}

}
