package ru.prolib.caelum.lib.data;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.math.BigInteger;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

public class TupleDataTest {
    private IMocksControl control;
    private BigInteger open, high, low, close, volume;
    private TupleData service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        open = BigInteger.valueOf(117726L);
        high = BigInteger.valueOf(120096L);
        low = BigInteger.valueOf(90580L);
        close = BigInteger.valueOf(115210L);
        volume = BigInteger.valueOf(10000L);
        service = new TupleData(open, high, low, close, 5, volume, 10);
    }
    
    @Test
    public void testGetters() {
        assertSame(open, service.open());
        assertSame(high, service.high());
        assertSame(low, service.low());
        assertSame(close, service.close());
        assertSame(volume, service.volume());
        assertEquals(5, service.decimals());
        assertEquals(10, service.volumeDecimals());
    }
    
    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals_SpecialCases() {
        assertTrue(service.equals(service));
        assertFalse(service.equals(null));
        assertFalse(service.equals(this));
    }
    
    @Test
    public void testEquals() {
        assertTrue(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(111111L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(222222L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(333333L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(444444L),
                5,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                6,
                BigInteger.valueOf(10000L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(777777L),
                10
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(117726L),
                BigInteger.valueOf(120096L),
                BigInteger.valueOf(90580L),
                BigInteger.valueOf(115210L),
                5,
                BigInteger.valueOf(10000L),
                11
            )));
        assertFalse(service.equals(new TupleData(
                BigInteger.valueOf(111111L),
                BigInteger.valueOf(222222L),
                BigInteger.valueOf(333333L),
                BigInteger.valueOf(444444L),
                6,
                BigInteger.valueOf(777777L),
                11
            )));
    }
    
    @Test
    public void testEquals_ShouldBeAbleToCompareWithAnyImplementation() {
        ITupleData tupleMock1 = control.createMock(ITupleData.class);
        expect(tupleMock1.open()).andStubReturn(open);
        expect(tupleMock1.high()).andStubReturn(high);
        expect(tupleMock1.low()).andStubReturn(low);
        expect(tupleMock1.close()).andStubReturn(close);
        expect(tupleMock1.volume()).andStubReturn(volume);
        expect(tupleMock1.decimals()).andStubReturn(5);
        expect(tupleMock1.volumeDecimals()).andStubReturn(10);
        ITupleData tupleMock2 = control.createMock(ITupleData.class);
        expect(tupleMock2.open()).andStubReturn(BigInteger.valueOf(115L));
        expect(tupleMock2.high()).andStubReturn(BigInteger.valueOf(240L));
        expect(tupleMock2.low()).andStubReturn(BigInteger.valueOf(112L));
        expect(tupleMock2.close()).andStubReturn(BigInteger.valueOf(180L));
        expect(tupleMock2.volume()).andStubReturn(BigInteger.valueOf(500L));
        expect(tupleMock2.decimals()).andStubReturn(7);
        expect(tupleMock2.volumeDecimals()).andStubReturn(11);
        control.replay();
        
        assertTrue(service.equals(tupleMock1));
        assertFalse(service.equals(tupleMock2));
        
        control.verify();
    }

}
