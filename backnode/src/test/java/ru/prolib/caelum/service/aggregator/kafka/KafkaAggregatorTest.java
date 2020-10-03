package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.service.AggregatorState.*;
import static ru.prolib.caelum.service.AggregatorType.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.GeneralConfigImpl;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		
	}
	
	IMocksControl control;
	GeneralConfigImpl gconf;
	KafkaAggregatorDescr descr;
	KafkaAggregatorConfig config;
	IRecoverableStreamsService streamsServiceMock;
	KafkaUtils utilsMock;
	AdminClient adminMock;
	KafkaStreamsRegistry registryMock;
	KafkaAggregatorEntry entryMock;
	Intervals intervals;
	KafkaAggregator service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsServiceMock = control.createMock(IRecoverableStreamsService.class);
		gconf = new GeneralConfigImpl();
		utilsMock = control.createMock(KafkaUtils.class);
		adminMock = control.createMock(AdminClient.class);
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		entryMock = control.createMock(KafkaAggregatorEntry.class);
		descr = new KafkaAggregatorDescr(ITEM, Interval.M1, "d-source", "d-target", "d-store");
		config = new KafkaAggregatorConfig(Interval.M1, gconf);
		service = new KafkaAggregator(descr, config, streamsServiceMock, utilsMock, registryMock);
	}
	
	@Test
	public void testCtor4() {
		assertEquals(descr, service.getDescriptor());
		assertEquals(config, service.getConfig());
		assertSame(streamsServiceMock, service.getStreamsService());
		assertSame(utilsMock, service.getUtils());
		assertSame(registryMock, service.getStreamsRegistry());
	}
	
	@Test
	public void testGetStatus() {
		expect(registryMock.getByInterval(Interval.M1)).andReturn(entryMock);
		expect(streamsServiceMock.getState()).andReturn(STARTING);
		expect(entryMock.isAvailable()).andReturn(false);
		expect(entryMock.getStreamsState()).andReturn(KafkaStreams.State.ERROR);
		control.replay();
		
		AggregatorStatus actual = service.getStatus();
		
		control.verify();
		AggregatorStatus expected = new AggregatorStatus("AK", Interval.M1, ITEM, STARTING,
				new KafkaAggregatorStatusInfo("d-source", "d-target", "d-store", false, State.ERROR));
		assertEquals(expected, actual);
	}
	
	@Test
	public void testClear_Global_ShouldAlsoClearTargetTopicIfDefined() {
		gconf.setKafkaBootstrapServers("kukaracha:2298")
			.setDefaultTimeout(180215L)
			.setAggregatorKafkaApplicationIdPrefix("myApp-")
			.setAggregatorKafkaStorePrefix("myStore-")
			.setAggregatorKafkaTargetTopicPrefix("myTarget-");
		expect(streamsServiceMock.stopAndWaitConfirm(180215L)).andReturn(true);
		expect(utilsMock.createAdmin(gconf)).andReturn(adminMock);
		utilsMock.deleteRecords(adminMock, "myApp-m1-myStore-m1-changelog", 180215L);
		utilsMock.deleteRecords(adminMock, "myTarget-m1", 180215L);
		adminMock.close();
		expect(streamsServiceMock.startAndWaitConfirm(180215L)).andReturn(true);
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}

	@Test
	public void testClear_Global_ShouldSkipClearingTargetTopicIfNotDefined() {
		gconf.setKafkaBootstrapServers("localhost:6257")
			.setDefaultTimeout(35193L)
			.setAggregatorKafkaApplicationIdPrefix("myApp-")
			.setAggregatorKafkaStorePrefix("myStore-")
			.setAggregatorKafkaTargetTopicPrefix("");
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(true);
		expect(utilsMock.createAdmin(gconf)).andReturn(adminMock);
		utilsMock.deleteRecords(adminMock, "myApp-m1-myStore-m1-changelog", 35193L);
		adminMock.close();
		expect(streamsServiceMock.startAndWaitConfirm(35193L)).andReturn(true);
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}
	
	@Test
	public void testClear_Global_ThrowsIfFailedToStopService() {
		gconf.setDefaultTimeout(35193L);
		expect(streamsServiceMock.stopAndWaitConfirm(35193L)).andReturn(false);
		control.replay();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.clear(true));
		assertEquals("Failed to stop streams service: M1", e.getMessage());
	}
	
	@Test
	public void testClear_Local_ShouldJustRestart() {
		gconf.setDefaultTimeout(117L);
		expect(streamsServiceMock.stopAndWaitConfirm(117L)).andReturn(true);
		expect(streamsServiceMock.startAndWaitConfirm(117L)).andReturn(true);
		control.replay();
		
		service.clear(false);
		
		control.verify();
	}

}
