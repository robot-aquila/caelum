package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;

import javax.ws.rs.core.StreamingOutput;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.ITuple;
import ru.prolib.caelum.service.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.service.itemdb.IItemDataRequest;
import ru.prolib.caelum.service.itemdb.IItemIterator;
import ru.prolib.caelum.service.symboldb.EventListRequest;
import ru.prolib.caelum.service.symboldb.SymbolListRequest;

@SuppressWarnings("unchecked")
public class StreamFactoryTest {
	IMocksControl control;
	JsonFactory jsonFactoryMock;
	ValueFormatter formatterMock;
	Clock clockMock;
	StreamFactory service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		jsonFactoryMock = control.createMock(JsonFactory.class);
		formatterMock = control.createMock(ValueFormatter.class);
		clockMock = control.createMock(Clock.class);
		service = new StreamFactory(jsonFactoryMock, formatterMock, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(jsonFactoryMock, service.getJsonFactory());
		assertSame(formatterMock, service.getFormatter());
		assertSame(clockMock, service.getClock());
	}
	
	@Test
	public void testCategoriesToJson() {
		ICloseableIterator<String> itMock = control.createMock(ICloseableIterator.class);
		
		StreamingOutput actual = service.categoriesToJson(itMock); 
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamCategoriesToJson.class)));
		StreamCategoriesToJson x = (StreamCategoriesToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
	}
	
	@Test
	public void testSymbolsToJson() {
		ICloseableIterator<String> itMock = control.createMock(ICloseableIterator.class);
		SymbolListRequest request = new SymbolListRequest("foo", 50);
		
		StreamingOutput actual = service.symbolsToJson(itMock, request);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamSymbolsToJson.class)));
		StreamSymbolsToJson x = (StreamSymbolsToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
		assertSame(request, x.getRequest());
	}
	
	@Test
	public void testItemsToJson() {
		IItemIterator itMock = control.createMock(IItemIterator.class);
		IItemDataRequest requestMock = control.createMock(IItemDataRequest.class);
		
		StreamingOutput actual = service.itemsToJson(itMock, requestMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamItemsToJson.class)));
		StreamItemsToJson x = (StreamItemsToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
		assertSame(requestMock, x.getRequest());
		assertSame(formatterMock, x.getFormatter());
	}

	@Test
	public void testTuplesToJson() {
		ICloseableIterator<ITuple> itMock = control.createMock(ICloseableIterator.class);
		AggregatedDataRequest requestMock = control.createMock(AggregatedDataRequest.class);
		
		StreamingOutput actual = service.tuplesToJson(itMock, requestMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamTuplesToJson.class)));
		StreamTuplesToJson x = (StreamTuplesToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
		assertSame(requestMock, x.getRequest());
		assertSame(formatterMock, x.getFormatter());
	}
	
	@Test
	public void testEventsToJson() {
		ICloseableIterator<Events> itMock = control.createMock(ICloseableIterator.class);
		EventListRequest request = new EventListRequest("foo@bar");
		
		StreamingOutput actual = service.eventsToJson(itMock, request);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamEventsToJson.class)));
		StreamEventsToJson x = (StreamEventsToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
		assertEquals(request, x.getRequest());
	}
	
	@Test
	public void testStringsToJson() {
		ICloseableIterator<String> itMock = control.createMock(ICloseableIterator.class);
		
		StreamingOutput actual = service.stringsToJson(itMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamStringsToJson.class)));
		StreamStringsToJson x = (StreamStringsToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
	}

}
