package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorService;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

@SuppressWarnings("unchecked")
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
	public void testRegisterSymbolUpdate() {
		symbolSvcMock.registerSymbolUpdate(new SymbolUpdate("foo@bar", 15272893L, new HashMap<>()));
		control.replay();
		
		service.registerSymbolUpdate(new SymbolUpdate("foo@bar", 15272893L, new HashMap<>()));
		
		control.verify();
	}
	
	@Test
	public void testFetch_AggrDataRequest() {
		AggregatedDataRequest requestMock = control.createMock(AggregatedDataRequest.class);
		ICloseableIterator<ITuple> resultMock = control.createMock(ICloseableIterator.class);
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
	public void testFetchSymbolUpdates() {
		ICloseableIterator<SymbolUpdate> resultMock = control.createMock(ICloseableIterator.class);
		expect(symbolSvcMock.listSymbolUpdates("kabucha@listed")).andReturn(resultMock);
		control.replay();
		
		assertSame(resultMock, service.fetchSymbolUpdates("kabucha@listed"));
		
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

}
