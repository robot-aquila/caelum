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

import ru.prolib.caelum.lib.IteratorStub;
import ru.prolib.caelum.service.SymbolListRequest;

public class StreamSymbolsToJsonTest {
	JsonFactory jsonFactory = new JsonFactory();
	IMocksControl control;
	Clock clockMock;
	ByteArrayOutputStream output;
	IteratorStub<String> iterator;
	SymbolListRequest request;
	StreamSymbolsToJson service;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		clockMock = control.createMock(Clock.class);
		output = new ByteArrayOutputStream();
		iterator = new IteratorStub<>(Arrays.asList("zulu", "charlie", "bobby"), true);
		request = new SymbolListRequest("foo", "zumba", 45);
		service = new StreamSymbolsToJson(jsonFactory, iterator, request, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(jsonFactory, service.getJsonFactory());
		assertSame(iterator, service.getIterator());
		assertSame(request, service.getRequest());
		assertSame(clockMock, service.getClock());
	}

	@Test
	public void testWrite() throws Exception {
		expect(clockMock.millis()).andReturn(157789937L);
		control.replay();
		
		service.write(output);
		
		control.verify();
		String actual = output.toString("UTF8");
		
		String expected = new StringBuilder()
				.append("{")
				.append("   \"time\": 157789937,")
				.append("   \"error\": false,")
				.append("   \"code\": 0,")
				.append("   \"message\": null,")
				.append("   \"data\": {")
				.append("      \"category\": \"foo\",")
				.append("      \"rows\": [")
				.append("         \"zulu\",")
				.append("         \"charlie\",")
				.append("         \"bobby\"")
				.append("      ]")
				.append("   }")
				.append("}")
				.toString();
		JSONAssert.assertEquals(expected, actual, true);
		assertTrue(iterator.closed());
	}

}
