package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.util.Arrays;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Tuple;

public class StreamTuplesToJsonTest {
	JsonFactory jsonFactory = new JsonFactory();
	IMocksControl control;
	Clock clockMock;
	ByteArrayOutputStream output;
	IteratorStub<ITuple> iterator;
	ValueFormatter formatter;
	AggregatedDataRequest request;
	StreamTuplesToJson service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		clockMock = control.createMock(Clock.class);
		output = new ByteArrayOutputStream();
		iterator = new IteratorStub<>(Arrays.asList(
				Tuple.ofDecimax15("bar", 1573939L, 120, 130, 110, 115, 2, 800, 0),
				Tuple.ofDecimax15("bar", 1573945L, 116, 127, 116, 125, 2, 450, 0),
				Tuple.ofDecimax15("bar", 1573950L, 125, 130, 120, 121, 2, 112, 0)
				), true);
		formatter = ValueFormatter.getInstance();
		request = new AggregatedDataRequest("bar", Period.H1, 1550000L, 1600000L, 30);
		service = new StreamTuplesToJson(jsonFactory, iterator, request, formatter, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(jsonFactory, service.getJsonFactory());
		assertSame(iterator, service.getIterator());
		assertSame(request, service.getRequest());
		assertSame(formatter, service.getFormatter());
		assertSame(clockMock, service.getClock());
	}

	@Test
	public void testWrite() throws Exception {
		expect(clockMock.millis()).andReturn(165000L);
		control.replay();
		
		service.write(output);
		
		control.verify();
		String actual = output.toString("UTF8");
		
		String expected = new StringBuilder()
				.append("{")
				.append("   \"time\": 165000,")
				.append("   \"error\": false,")
				.append("   \"code\": 0,")
				.append("   \"message\": null,")
				.append("   \"data\": {")
				.append("      \"symbol\": \"bar\",")
				.append("      \"period\": \"H1\",")
				.append("      \"from\": 1550000,")
				.append("      \"to\": 1600000,")
				.append("      \"limit\": 30,")
				.append("      \"format\": \"std\",")
				.append("      \"rows\": [")
				.append("         [ 1573939, \"1.20\", \"1.30\", \"1.10\", \"1.15\", \"800\" ],")
				.append("         [ 1573945, \"1.16\", \"1.27\", \"1.16\", \"1.25\", \"450\" ],")
				.append("         [ 1573950, \"1.25\", \"1.30\", \"1.20\", \"1.21\", \"112\" ]")
				.append("      ]")
				.append("   }")
				.append("}")
				.toString();
		JSONAssert.assertEquals(expected, actual, true);
		assertTrue(iterator.closed());
	}

}
