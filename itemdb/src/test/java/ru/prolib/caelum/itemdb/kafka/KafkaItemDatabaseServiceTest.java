package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.time.Clock;
import java.util.Arrays;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;

@SuppressWarnings("unchecked")
public class KafkaItemDatabaseServiceTest {
	IMocksControl control;
	KafkaUtils utilsMock;
	IItemIterator itMock;
	KafkaConsumer<String, KafkaItem> consumerMock;
	KafkaProducer<String, KafkaItem> producerMock;
	AdminClient adminMock;
	Clock clockMock;
	KafkaItemDatabaseConfig config;
	KafkaItemInfo ii;
	TopicPartition tp;
	KafkaItemDatabaseService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		utilsMock = control.createMock(KafkaUtils.class);
		itMock = control.createMock(IItemIterator.class);
		consumerMock = control.createMock(KafkaConsumer.class);
		producerMock = control.createMock(KafkaProducer.class);
		adminMock = control.createMock(AdminClient.class);
		clockMock = control.createMock(Clock.class);
		config = new KafkaItemDatabaseConfig();
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, 0L, 10000L);
		tp = new TopicPartition("caelum-item", 1);
		service = new KafkaItemDatabaseService(config, producerMock, utilsMock, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(config, service.getConfig());
		assertSame(producerMock, service.getProducer());
		assertSame(utilsMock, service.getUtils());
		assertSame(clockMock, service.getClock());
	}
	
	@Test
	public void testGetters_Ctor2() {
		service = new KafkaItemDatabaseService(config, producerMock);
		assertSame(config, service.getConfig());
		assertSame(producerMock, service.getProducer());
		assertSame(KafkaUtils.getInstance(), service.getUtils());
		assertEquals(Clock.systemUTC(), service.getClock());
	}
	
	@Test
	public void testFetch_InitialRequest_HasData_LimitFromRequest() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 27768919862L, 0L)).andReturn(450L);
		consumerMock.seek(tp, 450L);
		expect(utilsMock.createIterator(consumerMock, ii, 100, 30000000000L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 100)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_HasData_LimitOverridenFromConfig() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 27768919862L, 0L)).andReturn(450L);
		consumerMock.seek(tp, 450L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000, 30000000000L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 10000)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_ShouldUseDefaultLimitIfRequestedLimitIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "torba")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 77878166L, 0L)).andReturn(144L);
		consumerMock.seek(tp, 144L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000, 90887812L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("torba", 77878166L, 90887812L, null)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_ShouldBeOkIfTimeFromIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "torba")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, null, 0L)).andReturn(0L);
		consumerMock.seek(tp, 0L);
		expect(utilsMock.createIterator(consumerMock, ii, 200, 90887812L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("torba", null, 90887812L, 200)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_ShouldBeOkIfTimeToIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "torba")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		expect(utilsMock.getOffset(consumerMock, tp, 77878166L, 0L)).andReturn(144L);
		consumerMock.seek(tp, 144L);
		expect(utilsMock.createIterator(consumerMock, ii, 200, null, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("torba", 77878166L, null, 200)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_InitialRequest_NoData() {
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, null, null);
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		expect(utilsMock.createIteratorStub(consumerMock, ii, 4000, 30000000000L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequest("zuzba-15", 27768919862L, 30000000000L, 4000)));
		
		control.verify();
	}

	@Test
	public void testFetch_ContinueRequest_HasData_LimitFromRequest() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 1000L);
		expect(utilsMock.createIterator(consumerMock, ii, 100, 16789266L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 1000L, "xxx", 16789266L, 100)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_HasData_LimitOverridenFromConfig() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 3000L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000, 16789266L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 3000L, "xxx", 16789266L, 7000)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_ShouldUseDefaultLimitIfRequestedLimitIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 4000L);
		expect(utilsMock.createIterator(consumerMock, ii, 5000, 16789266L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 4000L, "xxx", 16789266L, null)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_ShouldBeOkIfMagicIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 2000L);
		expect(utilsMock.createIterator(consumerMock, ii, 100, 16789266L, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 2000L, null, 16789266L, 100)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_ShouldBeOkIfTimeToIsNull() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		consumerMock.assign(Arrays.asList(tp));
		consumerMock.seek(tp, 2500L);
		expect(utilsMock.createIterator(consumerMock, ii, 100, null, clockMock)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 2500L, "xxx", null, 100)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_NoData() {
		ii = new KafkaItemInfo("caelum-item", 2, "zuzba-15", 1, null, null);
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "zuzba-15")).andReturn(ii);
		expect(utilsMock.createIteratorStub(consumerMock, ii, 2000, 16789266L)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("zuzba-15", 425000L, "xxx", 16789266L, 2000)));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ContinueRequest_ShouldSkipQueryIfFromOffsetIsOutOfRange() {
		expect(utilsMock.createConsumer(config.getConsumerKafkaProperties())).andReturn(consumerMock);
		expect(utilsMock.getItemInfo(consumerMock, "caelum-item", "bumba")).andReturn(ii);
		expect(utilsMock.createIteratorStub(consumerMock, ii, 5000, null)).andReturn(itMock);
		control.replay();
		
		assertSame(itMock, service.fetch(new ItemDataRequestContinue("bumba", 128764L, "xxx", null, null)));
		
		control.verify();
	}
	
	@Test
	public void testRegisterItem() {
		expect(producerMock.send(new ProducerRecord<>("caelum-item", null, 162829349L, "foo",
				new KafkaItem(4500L, (byte)4, 100L, (byte)2, ItemType.LONG_REGULAR))))
			.andReturn(null); // does not matter
		control.replay();
		
		service.registerItem(ru.prolib.caelum.core.Item.ofDecimax15("foo", 162829349L, 4500, 4, 100, 2));
		
		control.verify();
	}
	
	@Test
	public void testClear() {
		expect(utilsMock.createAdmin(config.getAdminClientProperties())).andReturn(adminMock);
		utilsMock.deleteRecords(adminMock, "caelum-item", 10000L);
		adminMock.close();
		control.replay();
		
		service.clear();
		
		control.verify();
	}
	
}
