package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.lib.Interval.*;
import static ru.prolib.caelum.service.AggregatorState.*;
import static ru.prolib.caelum.service.AggregatorType.*;

import java.util.Arrays;
import java.util.List;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.EventsBuilder;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Item;
import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorService;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.symboldb.ISymbolService;

public class CaelumTest {
	IMocksControl control;
	KafkaAggregatorService aggrSvcMock;
	IItemDatabaseService itemDbSvcMock;
	ISymbolService symbolSvcMock;
	IExtension extMock1, extMock2, extMock3;
	List<IExtension> extensions;
	Caelum service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(KafkaAggregatorService.class);
		itemDbSvcMock = control.createMock(IItemDatabaseService.class);
		symbolSvcMock = control.createMock(ISymbolService.class);
		extMock1 = control.createMock(IExtension.class);
		extMock2 = control.createMock(IExtension.class);
		extMock3 = control.createMock(IExtension.class);
		extensions = Arrays.asList(extMock1, extMock2, extMock3);
		service = new Caelum(aggrSvcMock, itemDbSvcMock, symbolSvcMock, extensions);
	}
	
	@Test
	public void testGetters() {
		assertSame(aggrSvcMock, service.getAggregatorService());
		assertSame(itemDbSvcMock, service.getItemDatabaseService());
		assertSame(symbolSvcMock, service.getSymbolService());
		assertSame(extensions, service.getExtensions());
	}
	
	@Test
	public void testRegisterSymbol_S() {
		symbolSvcMock.registerSymbol("foo@bar");
		control.replay();
		
		service.registerSymbol("foo@bar");
		
		control.verify();
	}
	
	@Test
	public void testRegisterSymbol_L() {
		symbolSvcMock.registerSymbol(Arrays.asList("foo", "bar"));
		control.replay();
		
		service.registerSymbol(Arrays.asList("foo", "bar"));
		
		control.verify();
	}
	
	@Test
	public void testRegisterEvents_S() {
		Events e = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(15272893L)
				.withEvent(101, "pop")
				.withEvent(102, "gap")
				.build();
		symbolSvcMock.registerEvents(e);
		control.replay();
		
		service.registerEvents(e);
		
		control.verify();
	}
	
	@Test
	public void testRegisterEvents_L() {
		Events
			e1 = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(15272893L)
				.withEvent(101, "pop")
				.withEvent(102, "gap")
				.build(),
			e2 = new EventsBuilder()
				.withSymbol("zoo@spa")
				.withTime(14284415L)
				.withEvent(5004, "luna")
				.withEvent(5005, "44")
				.build();
		symbolSvcMock.registerEvents(Arrays.asList(e1, e2));
		control.replay();
		
		service.registerEvents(Arrays.asList(e1, e2));
		
		control.verify();
	}
	
	@Test
	public void testDeleteEvents_S() {
		Events e = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(15272893L)
				.withEvent(101, "pop")
				.withEvent(102, "gap")
				.build();
		symbolSvcMock.deleteEvents(e);
		control.replay();
		
		service.deleteEvents(e);
		
		control.verify();
	}
	
	@Test
	public void testDeleteEvents_L() {
		Events
			e1 = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(15272893L)
				.withEvent(101, "pop")
				.withEvent(102, "gap")
				.build(),
			e2 = new EventsBuilder()
				.withSymbol("zoo@spa")
				.withTime(14284415L)
				.withEvent(5004, "luna")
				.withEvent(5005, "44")
				.build();
		symbolSvcMock.deleteEvents(Arrays.asList(e1, e2));
		control.replay();
		
		service.deleteEvents(Arrays.asList(e1, e2));
		
		control.verify();
	}
	
	@Test
	public void testFetch_AggrDataRequest() {
		AggregatedDataRequest requestMock = control.createMock(AggregatedDataRequest.class);
		AggregatedDataResponse resultMock = control.createMock(AggregatedDataResponse.class);
		expect(aggrSvcMock.fetch(requestMock)).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetch(requestMock));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ItemDataRequest() {
		ItemDataRequest requestMock = control.createMock(ItemDataRequest.class);
		IItemIterator resultMock = control.createMock(IItemIterator.class);
		expect(itemDbSvcMock.fetch(requestMock)).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetch(requestMock));
		
		control.verify();
	}
	
	@Test
	public void testFetch_ItemDataRequestContinue() {
		ItemDataRequestContinue requestMock = control.createMock(ItemDataRequestContinue.class);
		IItemIterator resultMock = control.createMock(IItemIterator.class);
		expect(itemDbSvcMock.fetch(requestMock)).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetch(requestMock));
		
		control.verify();
	}
	
	@Test
	public void testRegisterItem() {
		Item item = Item.ofDecimax15("foo", 15739304L, 15000, 2, 1000, 4);
		itemDbSvcMock.registerItem(item);
		control.replay();
		
		service.registerItem(item);
		
		control.verify();
	}
	
	@Test
	public void testRegisterItem_List() {
		List<IItem> items = Arrays.asList(
				Item.ofDecimax15("foo", 15739304L, 15000, 2, 1000, 4),
				Item.ofDecimax15("bar", 15739305L,   280, 1,   50, 5)
			);
		itemDbSvcMock.registerItem(items);
		control.replay();
		
		service.registerItem(items);
		
		control.verify();
	}
	
	@Test
	public void testFetchCategories() {
		ICloseableIterator<String> resultMock = control.createMock(ICloseableIterator.class);
		expect(symbolSvcMock.listCategories()).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetchCategories());
		
		control.verify();
	}
	
	@Test
	public void testFetchSymbols() {
		ICloseableIterator<String> resultMock = control.createMock(ICloseableIterator.class);
		expect(symbolSvcMock.listSymbols(new SymbolListRequest("kappa", "kappa@foo", 250))).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetchSymbols(new SymbolListRequest("kappa", "kappa@foo", 250)));
		
		control.verify();
	}
	
	@Test
	public void testFetchEvents() {
		EventListRequest request = new EventListRequest("kabucha@listed");
		ICloseableIterator<Events> resultMock = control.createMock(ICloseableIterator.class);
		expect(symbolSvcMock.listEvents(request)).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetchEvents(request));
		
		control.verify();
	}
	
	@Test
	public void testClear() {
		symbolSvcMock.clear(true);
		itemDbSvcMock.clear(true);
		aggrSvcMock.clear(true);
		extMock1.clear();
		extMock2.clear();
		extMock3.clear();
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}
	
	@Test
	public void testGetAggregationIntervals() {
		expect(aggrSvcMock.getAggregationIntervals()).andReturn(Arrays.asList(M1, M2, M5));
		control.replay();
		
		List<Interval> actual = service.getAggregationIntervals();
		
		control.verify();
		assertEquals(Arrays.asList(M1, M2, M5), actual);
	}
	
	@Test
	public void testGetAggregatorStatus() {
		expect(aggrSvcMock.getAggregatorStatus()).andReturn(Arrays.asList(
				new AggregatorStatus("AK", M1, ITEM, CREATED, null),
				new AggregatorStatus("AK", H1, ITEM, CREATED, null)
			));
		control.replay();
		
		List<AggregatorStatus> actual = service.getAggregatorStatus();
		
		control.verify();
		List<AggregatorStatus> expected = Arrays.asList(
				new AggregatorStatus("AK", M1, ITEM, CREATED, null),
				new AggregatorStatus("AK", H1, ITEM, CREATED, null)
			);
		assertEquals(expected, actual);
	}

}
