package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.ITuple;

@SuppressWarnings("unchecked")
public class AggregatedDataResponseTest {
	IMocksControl control;
	ICloseableIterator<ITuple> itMock1, itMock2;
	HostInfo hostInfo1, hostInfo2;
	AggregatedDataResponse service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		itMock1 = control.createMock(ICloseableIterator.class);
		itMock2 = control.createMock(ICloseableIterator.class);
		hostInfo1 = new HostInfo("zulu24", 1234);
		hostInfo2 = new HostInfo("charlie", 2345);
		service = new AggregatedDataResponse(hostInfo1, itMock1);
	}
	
	@Test
	public void testCtor2() {
		assertFalse(service.askAnotherHost());
		assertEquals(hostInfo1, service.getHostInfo());
		assertSame(itMock1, service.getResult());
	}
	
	@Test
	public void testCtor1() {
		service = new AggregatedDataResponse(hostInfo2);
		assertTrue(service.askAnotherHost());
		assertEquals(hostInfo2, service.getHostInfo());
		assertNull(service.getResult());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(new AggregatedDataResponse(hostInfo2).equals(new AggregatedDataResponse(hostInfo2)));
		assertTrue(service.equals(service));
		assertTrue(service.equals(new AggregatedDataResponse(hostInfo1, itMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(new AggregatedDataResponse(hostInfo2).equals(new AggregatedDataResponse(hostInfo1)));
		assertFalse(service.equals(new AggregatedDataResponse(hostInfo2, itMock1)));
		assertFalse(service.equals(new AggregatedDataResponse(hostInfo1, itMock2)));
		assertFalse(service.equals(new AggregatedDataResponse(hostInfo2, itMock2)));
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(77801, 1001)
				.append(false)
				.append(hostInfo1)
				.append(itMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testToString() {
		String expected = new StringBuilder()
				.append("AggregatedDataResponse[")
				.append("askAnotherHost=false,")
				.append("hostInfo=").append(hostInfo1).append(",")
				.append("result=").append(itMock1)
				.append("]")
				.toString();
		
		assertEquals(expected, service.toString());
	}

}
