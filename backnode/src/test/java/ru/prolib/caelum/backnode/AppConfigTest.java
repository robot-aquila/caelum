package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.itemdb.kafka.KafkaItemDatabaseConfig;

public class AppConfigTest {
	IMocksControl control;
	ItemAggregatorConfig itemAggrConfMock;
	KafkaItemDatabaseConfig itemDbConfMock;
	AppConfig service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		itemAggrConfMock = control.createMock(ItemAggregatorConfig.class);
		itemDbConfMock = control.createMock(KafkaItemDatabaseConfig.class);
		service = new AppConfig(itemAggrConfMock, itemDbConfMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(itemAggrConfMock, service.getItemAggregatorConfig());
		assertSame(itemDbConfMock, service.getItemDatabaseConfig());
	}
	
	@Test
	public void testCtor0() {
		service = new AppConfig();
		assertNotNull(service.getItemAggregatorConfig());
		assertNotNull(service.getItemDatabaseConfig());
	}
	
	@Test
	public void testLoad1() throws Exception {
		itemAggrConfMock.load("app.backnode.properties", "foo.bar");
		itemDbConfMock.load("app.backnode.properties", "foo.bar");
		control.replay();
		
		service.load("foo.bar");
		
		control.verify();
	}

	@Test
	public void testLoad0() throws Exception {
		itemAggrConfMock.load("app.backnode.properties", null);
		itemDbConfMock.load("app.backnode.properties", null);
		control.replay();
		
		service.load();
		
		control.verify();
	}

}
