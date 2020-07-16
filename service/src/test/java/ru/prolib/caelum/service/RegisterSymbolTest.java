package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.symboldb.ISymbolService;

public class RegisterSymbolTest {
	IMocksControl control;
	ISymbolService symbolSvcMock;
	ISymbolCache symbolCacheMock;
	RegisterSymbol service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		symbolSvcMock = control.createMock(ISymbolService.class);
		symbolCacheMock = control.createMock(ISymbolCache.class);
		service = new RegisterSymbol("foo@bar", symbolSvcMock, symbolCacheMock);
	}
	
	@Test
	public void testGetters() {
		assertEquals("foo@bar", service.getSymbol());
		assertSame(symbolSvcMock, service.getSymbolService());
		assertSame(symbolCacheMock, service.getSymbolCache());
	}
	
	@Test
	public void testRun() {
		symbolSvcMock.registerSymbol("foo@bar");
		symbolCacheMock.setRegistered("foo@bar");
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(117295, 13)
				.append("foo@bar")
				.append(symbolSvcMock)
				.append(symbolCacheMock)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		ISymbolService symbolSvcMock2 = control.createMock(ISymbolService.class);
		ISymbolCache symbolCacheMock2 = control.createMock(ISymbolCache.class);
		assertTrue(service.equals(service));
		assertTrue(service.equals(new RegisterSymbol("foo@bar", symbolSvcMock, symbolCacheMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new RegisterSymbol("bar@bar", symbolSvcMock2, symbolCacheMock)));
		assertFalse(service.equals(new RegisterSymbol("foo@bar", symbolSvcMock, symbolCacheMock2)));
		assertFalse(service.equals(new RegisterSymbol("bar@bar", symbolSvcMock2, symbolCacheMock2)));
		
	}

}
