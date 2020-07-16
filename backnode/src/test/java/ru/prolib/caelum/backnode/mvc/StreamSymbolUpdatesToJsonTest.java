package ru.prolib.caelum.backnode.mvc;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class StreamSymbolUpdatesToJsonTest {
	
	static Map<Integer, String> toMap(Object... args) {
		if ( args.length % 2 != 0 ) {
			throw new IllegalArgumentException();
		}
		Map<Integer, String> result = new LinkedHashMap<>();
		for ( int i = 0; i < args.length / 2; i ++ ) {
			result.put((Integer) args[i * 2], (String) args[i * 2 + 1]);
		}
		return result;
	}
	
	static SymbolUpdate U(String symbol, long time, Object... args) {
		return new SymbolUpdate(symbol, time, toMap(args));
	}
	
	JsonFactory jsonFactory = new JsonFactory();
	IMocksControl control;
	Clock clockMock;
	ByteArrayOutputStream output;
	IteratorStub<SymbolUpdate> iterator;
	StreamSymbolUpdatesToJson service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		clockMock = control.createMock(Clock.class);
		output = new ByteArrayOutputStream();
		iterator = new IteratorStub<>(Arrays.asList(
				U("foo", 16899263L, 30, "foo", 31, "bar", 32, "buz"),
				U("foo", 16899350L, 11, "ups", 12, "dup", 13, "boo"),
				U("foo", 16899400L, 30, "gap", 32, "goo", 33, "pop")
			), true);
		service = new StreamSymbolUpdatesToJson(jsonFactory, iterator, "foo@bar", clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(jsonFactory, service.getJsonFactory());
		assertSame(iterator, service.getIterator());
		assertSame("foo@bar", service.getRequest());
		assertSame(clockMock, service.getClock());
	}

	@Test
	public void testWrite() throws Exception {
		expect(clockMock.millis()).andReturn(15798920043L);
		control.replay();
		
		service.write(output);
		
		control.verify();
		String actual = output.toString("UTF8");
		
		String expected = new StringBuilder()
				.append("{")
				.append("   \"time\": 15798920043,")
				.append("   \"error\": false,")
				.append("   \"code\": 0,")
				.append("   \"message\": null,")
				.append("   \"data\": {")
				.append("      \"symbol\": \"foo@bar\",")
				.append("      \"rows\": [")
				.append("        {\"time\": 16899263,\"tokens\":{\"30\": \"foo\", \"31\": \"bar\", \"32\": \"buz\"}},")
				.append("        {\"time\": 16899350,\"tokens\":{\"11\": \"ups\", \"12\": \"dup\", \"13\": \"boo\"}},")
				.append("        {\"time\": 16899400,\"tokens\":{\"30\": \"gap\", \"32\": \"goo\", \"33\": \"pop\"}}")
				.append("      ]")
				.append("   }")
				.append("}")
				.toString();
		JSONAssert.assertEquals(expected, actual, true);
		assertTrue(iterator.closed());
	}

}
