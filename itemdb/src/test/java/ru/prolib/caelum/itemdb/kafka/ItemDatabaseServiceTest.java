package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;

@SuppressWarnings("unchecked")
public class ItemDatabaseServiceTest {
	IMocksControl control;
	KafkaUtils utilsMock;
	IItemIterator itMock;
	KafkaConsumer<String, KafkaItem> consumerMock;
	ItemDatabaseConfig config;
	KafkaItemInfo ii;
	TopicPartition tp;
	ItemDatabaseService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		utilsMock = control.createMock(KafkaUtils.class);
		itMock = control.createMock(IItemIterator.class);
		consumerMock = control.createMock(KafkaConsumer.class);
		config = new ItemDatabaseConfig();
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, 0L, 10000L);
		tp = new TopicPartition("caelum-item", 1);
		service = new ItemDatabaseService(config, utilsMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(config, service.getConfig());
	}
	
	@Test
	public void testFetch_InitialRequest_HasData_LimitFromRequest() {
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 27768919862L, 0L)).andReturn(450L);
		consumerMock.seek(tp, 450L);
		expect(utilsMock.createIterator(consumerMock, ii, 100L, 30000000000L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 100L)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_HasData_LimitOverridenFromConfig() {
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 27768919862L, 0L)).andReturn(450L);
		consumerMock.seek(tp, 450L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000L, 30000000000L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 10000L)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_NoData() {
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, null, null);
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		expect(utilsMock.createIteratorStub(consumerMock, ii, 4000L, 30000000000L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 4000L)));
		
		control.verify();
	}

	@Test
	public void testFetch_ContinueRequest_HasData_LimitFromRequest() {
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 425000L);
		expect(utilsMock.createIterator(consumerMock, ii, 100L, 16789266L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 425000L, "xxx", 16789266L, 100L)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_HasData_LimitOverridenFromConfig() {
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 425000L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000L, 16789266L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 425000L, "xxx", 16789266L, 7000L)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_NoData() {
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, null, null);
		expect(utilsMock.createConsumer(config.getKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		expect(utilsMock.createIteratorStub(consumerMock, ii, 2000L, 16789266L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 425000L, "xxx", 16789266L, 2000L)));
		
		control.verify();
	}

}
