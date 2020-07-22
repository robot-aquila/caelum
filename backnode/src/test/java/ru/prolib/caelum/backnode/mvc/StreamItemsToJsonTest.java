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

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataResponse;
import ru.prolib.caelum.itemdb.ItemIteratorStub;

public class StreamItemsToJsonTest {
	JsonFactory jsonFactory = new JsonFactory();
	IMocksControl control;
	Clock clockMock;
	ByteArrayOutputStream output;
	ItemIteratorStub iterator;
	ValueFormatter formatter;
	ItemDataRequest request;
	StreamItemsToJson service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		clockMock = control.createMock(Clock.class);
		output = new ByteArrayOutputStream();
		formatter = ValueFormatter.getInstance();
		iterator = new ItemIteratorStub(Arrays.asList(
				Item.ofDecimax15("foo", 1583830L, 250L, 2, 10L, 3),
				Item.ofDecimax15("foo", 1583850L, 245L, 2, 20L, 3),
				Item.ofDecimax15("foo", 1583870L, 240L, 2, 15L, 3)),
				new ItemDataResponse(500, "xxx"),
				true
			);
		request = new ItemDataRequest("foo", 1580000L, 1600000L, 500);
		service = new StreamItemsToJson(jsonFactory, iterator, request, formatter, clockMock);
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
		expect(clockMock.millis()).andReturn(1628399L);
		control.replay();
		
		service.write(output);
		
		control.verify();
		String actual = output.toString("UTF8");
		
		String expected = new StringBuilder()
				.append("{")
				.append("   \"time\": 1628399,")
				.append("   \"error\": false,")
				.append("   \"code\": 0,")
				.append("   \"message\": null,")
				.append("   \"data\": {")
				.append("      \"symbol\": \"foo\",")
				.append("      \"format\": \"std\",")
				.append("      \"rows\": [")
				.append("         [ 1583830, \"2.50\", \"0.010\" ],")
				.append("         [ 1583850, \"2.45\", \"0.020\" ],")
				.append("         [ 1583870, \"2.40\", \"0.015\" ]")
				.append("      ],")
				.append("      \"magic\": \"xxx\",")
				.append("      \"fromOffset\": 501")
				.append("   }")
				.append("}")
				.toString();
		JSONAssert.assertEquals(expected, actual, true);
		assertTrue(iterator.closed());
	}
	
	@Test
	public void testWrite_ShouldBeOkEvenIfTimeFromAndTimeToAndLimitAreNull() throws Exception {
		request = new ItemDataRequest("foo", null, null, null);
		service = new StreamItemsToJson(jsonFactory, iterator, request, formatter, clockMock);
		expect(clockMock.millis()).andReturn(1628399L);
		control.replay();
		
		service.write(output);
		
		control.verify();
		String actual = output.toString("UTF8");
		
		String expected = new StringBuilder()
				.append("{")
				.append("   \"time\": 1628399,")
				.append("   \"error\": false,")
				.append("   \"code\": 0,")
				.append("   \"message\": null,")
				.append("   \"data\": {")
				.append("      \"symbol\": \"foo\",")
				.append("      \"format\": \"std\",")
				.append("      \"rows\": [")
				.append("         [ 1583830, \"2.50\", \"0.010\" ],")
				.append("         [ 1583850, \"2.45\", \"0.020\" ],")
				.append("         [ 1583870, \"2.40\", \"0.015\" ]")
				.append("      ],")
				.append("      \"magic\": \"xxx\",")
				.append("      \"fromOffset\": 501")
				.append("   }")
				.append("}")
				.toString();
		JSONAssert.assertEquals(expected, actual, true);
		assertTrue(iterator.closed());
	}

}
