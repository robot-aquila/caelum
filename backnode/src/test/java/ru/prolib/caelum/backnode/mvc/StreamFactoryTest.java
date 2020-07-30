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

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.itemdb.IItemDataRequest;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

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
	public void testSymbolUpdatesToJson() {
		ICloseableIterator<SymbolUpdate> itMock = control.createMock(ICloseableIterator.class);
		
		StreamingOutput actual = service.symbolUpdatesToJson(itMock, "foo@bar");
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(StreamSymbolUpdatesToJson.class)));
		StreamSymbolUpdatesToJson x = (StreamSymbolUpdatesToJson) actual;
		assertSame(jsonFactoryMock, x.getJsonFactory());
		assertSame(clockMock, x.getClock());
		assertSame(itMock, x.getIterator());
		assertEquals("foo@bar", x.getRequest());
	}

}
