package ru.prolib.caelum.itemdb.kafka.utils;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.core.AbstractConfig;

public class KafkaStreamsServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaStreams streamsMock;
	AbstractConfig configMock;
	KafkaStreamsService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsMock = control.createMock(KafkaStreams.class);
		configMock = control.createMock(AbstractConfig.class);
		service = new KafkaStreamsService(streamsMock, "Test service", configMock);
	}
	
	@Test
	public void testStart_WithConfig() {
		configMock.print(KafkaStreamsService.logger);
		streamsMock.cleanUp();
		streamsMock.start();
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStart_WoConfig() {
		service = new KafkaStreamsService(streamsMock, "Bets service");
		streamsMock.cleanUp();
		streamsMock.start();
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStop() {
		streamsMock.close();
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(18989261, 15)
				.append(streamsMock)
				.append("Test service")
				.append(configMock)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		KafkaStreams streamsMock2 = control.createMock(KafkaStreams.class);
		AbstractConfig configMock2 = control.createMock(AbstractConfig.class);
		
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaStreamsService(streamsMock, "Test service", configMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaStreamsService(streamsMock2, "Test service", configMock)));
		assertFalse(service.equals(new KafkaStreamsService(streamsMock, "Fest service", configMock)));
		assertFalse(service.equals(new KafkaStreamsService(streamsMock, "Test service", configMock2)));
		assertFalse(service.equals(new KafkaStreamsService(streamsMock2, "Fest service", configMock2)));
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("KafkaStreamsService[serviceName=Test service")
				.append(",streams=").append(streamsMock)
				.append(",config=").append(configMock)
				.append("]")
				.toString();

		assertEquals(expected, service.toString());
	}

}
