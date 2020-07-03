package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.kafka.AggregatorService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.symboldb.ISymbolService;

public class CaelumBuilderTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	AggregatorService aggrSvcMock;
	IItemDatabaseService itemDbSvcMock;
	ISymbolService symbolSvcMock;
	CaelumBuilder service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(AggregatorService.class);
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

}
