package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorConfig;
import ru.prolib.caelum.itemdb.kafka.KafkaItemDatabaseConfig;

public class AppConfigTest {
	IMocksControl control;
	AppConfig service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		service = new AppConfig();
	}

}
