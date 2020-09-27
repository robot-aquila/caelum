package ru.prolib.caelum.service.itemdb.kafka.utils;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.kafka.KafkaItem;

@SuppressWarnings("unchecked")
public class KafkaProducerServiceTest {
	IMocksControl control;
	KafkaProducer<String, KafkaItem> producerMock1, producerMock2;
	KafkaProducerService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		producerMock1 = control.createMock(KafkaProducer.class);
		producerMock2 = control.createMock(KafkaProducer.class);
		service = new KafkaProducerService(producerMock1);
	}
	
	@Test
	public void testStart() {
		producerMock1.initTransactions();
		control.replay();
		
		service.start();
		
		control.verify();
	}

	@Test
	public void testStop() {
		producerMock1.close();
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(90017625, 43)
				.append(producerMock1)
				.build();

		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaProducerService(producerMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaProducerService(producerMock2)));
	}

}
