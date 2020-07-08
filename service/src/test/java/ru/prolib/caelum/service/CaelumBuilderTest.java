package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorService;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class CaelumBuilderTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaAggregatorService aggrSvcMock;
	IItemDatabaseService itemDbSvcMock;
	ISymbolService symbolSvcMock;
	CaelumBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(KafkaAggregatorService.class);
		itemDbSvcMock = control.createMock(IItemDatabaseService.class);
		symbolSvcMock = control.createMock(ISymbolService.class);
		service = new CaelumBuilder();
	}
	
	@Test
	public void testBuild() {
		assertSame(service, service.withAggregatorService(aggrSvcMock));
		assertSame(service, service.withItemDatabaseService(itemDbSvcMock));
		assertSame(service, service.withSymbolService(symbolSvcMock));
		
		ICaelum actual = service.build();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Caelum.class)));
		Caelum o = (Caelum) actual;
		assertSame(aggrSvcMock, o.getAggregatorService());
		assertSame(itemDbSvcMock, o.getItemDatabaseService());
		assertSame(symbolSvcMock, o.getSymbolService());
	}
	
	@Test
	public void testBuild_ThrowsIfAggregatorServiceWasNotDefined() {
		eex.expect(NullPointerException.class);
		eex.expectMessage("Aggregator service was not defined");
		service.withItemDatabaseService(itemDbSvcMock);
		service.withSymbolService(symbolSvcMock);
		
		service.build();
	}
	
	@Test
	public void testBuild_ThrowsIfItemDatabaseServiceWasNotDefined() {
		eex.expect(NullPointerException.class);
		eex.expectMessage("ItemDB service was not defined");
		service.withAggregatorService(aggrSvcMock);
		service.withSymbolService(symbolSvcMock);
		
		service.build();
	}

	@Test
	public void testBuild_ThrowsIfSymbolServiceWasNotDefined() {
		eex.expect(NullPointerException.class);
		eex.expectMessage("Symbol service was not defined");
		service.withAggregatorService(aggrSvcMock);
		service.withItemDatabaseService(itemDbSvcMock);
		
		service.build();
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
	public void testBuild3() throws Exception {
		CompositeService servicesMock = control.createMock(CompositeService.class);
		IItemDatabaseServiceBuilder itemDbSvcBuilderMock = control.createMock(IItemDatabaseServiceBuilder.class);
		IAggregatorServiceBuilder aggrSvcBuilderMock = control.createMock(IAggregatorServiceBuilder.class);
		ISymbolServiceBuilder symbolSvcBuilderMock = control.createMock(ISymbolServiceBuilder.class);
		service = partialMockBuilder(CaelumBuilder.class)
				.addMockedMethod("createItemDatabaseServiceBuilder")
				.addMockedMethod("createAggregatorServiceBuilder")
				.addMockedMethod("createSymbolServiceBuilder")
				.createMock();
		expect(service.createAggregatorServiceBuilder()).andReturn(aggrSvcBuilderMock);
		expect(aggrSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(aggrSvcMock);
		expect(service.createItemDatabaseServiceBuilder()).andReturn(itemDbSvcBuilderMock);
		expect(itemDbSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(itemDbSvcMock);
		expect(service.createSymbolServiceBuilder()).andReturn(symbolSvcBuilderMock);
		expect(symbolSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(symbolSvcMock);
		control.replay();
		replay(service);
		
		ICaelum actual = service.build("foo.props", "bar.props", servicesMock);
		
		verify(service);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Caelum.class)));
		Caelum x = (Caelum) actual;
		assertSame(aggrSvcMock, x.getAggregatorService());
		assertSame(itemDbSvcMock, x.getItemDatabaseService());
		assertSame(symbolSvcMock, x.getSymbolService());
	}

}
