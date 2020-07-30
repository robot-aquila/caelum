package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorService;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.ExecutorService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class CaelumBuilderTest {
	IMocksControl control;
	KafkaAggregatorService aggrSvcMock;
	IItemDatabaseService itemDbSvcMock;
	ISymbolService symbolSvcMock;
	java.util.concurrent.ExecutorService executorMock;
	CompositeService servicesMock;
	IItemDatabaseServiceBuilder itemDbSvcBuilderMock;
	IAggregatorServiceBuilder aggrSvcBuilderMock;
	ISymbolServiceBuilder symbolSvcBuilderMock;
	ISymbolCache symbolCacheMock;
	CaelumBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(KafkaAggregatorService.class);
		itemDbSvcMock = control.createMock(IItemDatabaseService.class);
		symbolSvcMock = control.createMock(ISymbolService.class);
		executorMock = control.createMock(java.util.concurrent.ExecutorService.class);
		servicesMock = control.createMock(CompositeService.class);
		itemDbSvcBuilderMock = control.createMock(IItemDatabaseServiceBuilder.class);
		aggrSvcBuilderMock = control.createMock(IAggregatorServiceBuilder.class);
		symbolSvcBuilderMock = control.createMock(ISymbolServiceBuilder.class);
		symbolCacheMock = control.createMock(ISymbolCache.class);
		service = new CaelumBuilder();
		mockedService = partialMockBuilder(CaelumBuilder.class)
				.addMockedMethod("createItemDatabaseServiceBuilder")
				.addMockedMethod("createAggregatorServiceBuilder")
				.addMockedMethod("createSymbolServiceBuilder")
				.addMockedMethod("createExecutor")
				.addMockedMethod("createSymbolCache")
				.createMock();
	}
	
	@Test
	public void testCreateItemDatabaseServiceBuilder() {
		IItemDatabaseServiceBuilder actual = service.createItemDatabaseServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemDatabaseServiceBuilder.class)));
	}
	
	@Test
	public void testCreateAggregatorServiceBuilder() {
		IAggregatorServiceBuilder actual = service.createAggregatorServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(AggregatorServiceBuilder.class)));
	}
	
	@Test
	public void testCreateSymbolServiceBuilder() {
		ISymbolServiceBuilder actual = service.createSymbolServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(SymbolServiceBuilder.class)));
	}
	
	@Test
	public void testCreateExecutor() {
		java.util.concurrent.ExecutorService actual = service.createExecutor();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ThreadPoolExecutor.class)));
	}
	
	@Test
	public void testCreateSymbolCache() {
		Capture<ExecutorService> cap = newCapture();
		expect(servicesMock.register(capture(cap))).andReturn(servicesMock);
		control.replay();
		
		ISymbolCache actual = service.createSymbolCache(symbolSvcMock, servicesMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(SymbolCache.class)));
		java.util.concurrent.ExecutorService actual_executor = cap.getValue().getExecutor();
		assertEquals(15000L, cap.getValue().getShutdownTimeout());
		assertThat(actual_executor, is(instanceOf(ThreadPoolExecutor.class)));
		SymbolCache x = (SymbolCache) actual;
		assertSame(symbolSvcMock, x.getSymbolService());
		assertSame(actual_executor, x.getExecutor());
		assertThat(x.getMarkers(), is(instanceOf(ConcurrentHashMap.class)));
	}
	
	@Test
	public void testBuild3() throws Exception {
		expect(mockedService.createSymbolServiceBuilder()).andReturn(symbolSvcBuilderMock);
		expect(symbolSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(symbolSvcMock);
		expect(mockedService.createAggregatorServiceBuilder()).andReturn(aggrSvcBuilderMock);
		expect(aggrSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(aggrSvcMock);
		expect(mockedService.createItemDatabaseServiceBuilder()).andReturn(itemDbSvcBuilderMock);
		expect(itemDbSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(itemDbSvcMock);
		expect(mockedService.createSymbolCache(symbolSvcMock, servicesMock)).andReturn(symbolCacheMock);
		control.replay();
		replay(mockedService);
		
		ICaelum actual = mockedService.build("foo.props", "bar.props", servicesMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Caelum.class)));
		Caelum x = (Caelum) actual;
		assertSame(aggrSvcMock, x.getAggregatorService());
		assertSame(itemDbSvcMock, x.getItemDatabaseService());
		assertSame(symbolSvcMock, x.getSymbolService());
		assertSame(symbolCacheMock, x.getSymbolCache());
	}

}
