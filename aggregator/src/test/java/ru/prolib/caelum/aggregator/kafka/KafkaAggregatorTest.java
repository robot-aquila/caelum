package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.aggregator.AggregatorState.*;
import static ru.prolib.caelum.aggregator.AggregatorType.*;

import org.apache.kafka.clients.admin.AdminClient;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		
	}
	
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaAggregatorDescr descr;
	KafkaAggregatorConfig config;
	IRecoverableStreamsService streamsServiceMock;
	KafkaUtils utilsMock;
	AdminClient adminMock;
	Periods periods;
	KafkaAggregator service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsServiceMock = control.createMock(IRecoverableStreamsService.class);
		utilsMock = control.createMock(KafkaUtils.class);
		adminMock = control.createMock(AdminClient.class);
		descr = new KafkaAggregatorDescr(ITEM, Period.M1, "d-source", "d-target", "d-store");
		config = new KafkaAggregatorConfig(periods = new Periods());
		config.getProperties().put(KafkaAggregatorConfig.AGGREGATION_PERIOD, "M1");
		config.getProperties().put(KafkaAggregatorConfig.APPLICATION_ID_PREFIX, "myApp-");
		config.getProperties().put(KafkaAggregatorConfig.AGGREGATION_STORE_PREFIX, "myStore-");
		config.getProperties().put(KafkaAggregatorConfig.TARGET_TOPIC_PREFIX, "myTarget-");
		config.getProperties().put(KafkaAggregatorConfig.DEFAULT_TIMEOUT, "35193");
		service = new KafkaAggregator(descr, config, streamsServiceMock, utilsMock);
	}
	
	@Test
	public void testCtor4() {
		assertEquals(descr, service.getDescriptor());
		assertEquals(config, service.getConfig());
		assertSame(streamsServiceMock, service.getStreamsService());
		assertSame(utilsMock, service.getUtils());
	}
	
	@Test
	public void testGetStatus() {
		expect(streamsServiceMock.getState()).andReturn(STARTING);
		control.replay();
		
		AggregatorStatus actual = service.getStatus();
		
		control.verify();
		AggregatorStatus expected = new AggregatorStatus(Period.M1,
				ITEM, STARTING, "source=d-source target=d-target store=d-store");
		assertEquals(expected, actual);
	}
	
	@Test
	public void testClear_Global_ShouldAlsoClearTargetTopicIfDefined() {
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(true);
		expect(utilsMock.createAdmin(config.getAdminClientProperties())).andReturn(adminMock);
		utilsMock.deleteRecords(adminMock, "myApp-m1-myStore-m1-changelog", 35193L);
		utilsMock.deleteRecords(adminMock, "myTarget-m1", 35193L);
		adminMock.close();
		expect(streamsServiceMock.startAndWaitConfirm(35193L)).andReturn(true);
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}

	@Test
	public void testClear_Global_ShouldSkipClearingTargetTopicIfNotDefined() {
		config.getProperties().put(KafkaAggregatorConfig.TARGET_TOPIC_PREFIX, "");
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(true);
		expect(utilsMock.createAdmin(config.getAdminClientProperties())).andReturn(adminMock);
		utilsMock.deleteRecords(adminMock, "myApp-m1-myStore-m1-changelog", 35193L);
		adminMock.close();
		expect(streamsServiceMock.startAndWaitConfirm(35193L)).andReturn(true);
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}
	
	@Test
	public void testClear_Global_ThrowsIfFailedToStopService() {
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(false);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Failed to stop streams service: M1");
		
		service.clear(true);
	}
	
	@Test
	public void testClear_Local_ShouldJustRestart() {
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(true);
		expect(streamsServiceMock.startAndWaitConfirm(35193L)).andReturn(true);
		control.replay();
		
		service.clear(false);
		
		control.verify();
	}

}
