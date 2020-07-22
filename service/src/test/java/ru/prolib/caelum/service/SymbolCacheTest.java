package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.symboldb.ISymbolService;

public class SymbolCacheTest {
	IMocksControl control;
	ISymbolService symbolSvcMock1, symbolSvcMock2;
	ExecutorService executorMock1, executorMock2;
	Map<String, Boolean> markers1, markers2;
	SymbolCache service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		symbolSvcMock1 = control.createMock(ISymbolService.class);
		symbolSvcMock2 = control.createMock(ISymbolService.class);
		executorMock1 = control.createMock(ExecutorService.class);
		executorMock2 = control.createMock(ExecutorService.class);
		markers1 = new LinkedHashMap<>();
		markers2 = new LinkedHashMap<>();
		service = new SymbolCache(symbolSvcMock1, executorMock1, markers1);
	}
	
	@Test
	public void testGetters() {
		assertSame(symbolSvcMock1, service.getSymbolService());
		assertSame(executorMock1, service.getExecutor());
		assertSame(markers1, service.getMarkers());
	}
	
	@Test
	public void testRegisterSymbol_SkipIfAlreadyRegistered() {
		markers1.put("foo@bar", true);
		control.replay();
		
		service.registerSymbol("foo@bar");
		
		control.verify();
	}
	
	@Test
	public void testRegisterSymbol_RunTaskIfNotRegistered() {
		executorMock1.execute(new RegisterSymbol("foo@bar", symbolSvcMock1, service));
		control.replay();
		
		service.registerSymbol("foo@bar");
		
		control.verify();
	}

	@Test
	public void testSetRegistered() {
		control.replay();
		
		service.setRegistered("foo@bar");
		
		control.verify();
		assertTrue(markers1.containsKey("foo@bar"));
	}
	
	@Test
	public void testClear() {
		markers1.put("foo@bar", true);
		markers1.put("foo@buz", true);

		service.clear();
		
		assertEquals(0, markers1.size());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(9861653, 37)
				.append(symbolSvcMock1)
				.append(executorMock1)
				.append(markers1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		markers1.put("foo@bar", true);
		markers1.put("foo@buz", true);
		markers2.put("pan@top", true);
		markers2.put("pan@gap", true);
		assertTrue(service.equals(service));
		assertTrue(service.equals(new SymbolCache(symbolSvcMock1, executorMock1, markers1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new SymbolCache(symbolSvcMock2, executorMock1, markers1)));
		assertFalse(service.equals(new SymbolCache(symbolSvcMock1, executorMock2, markers1)));
		assertFalse(service.equals(new SymbolCache(symbolSvcMock1, executorMock1, markers2)));
		assertFalse(service.equals(new SymbolCache(symbolSvcMock2, executorMock2, markers2)));
	}
	
}
