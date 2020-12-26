package ru.prolib.caelum.lib.data;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Bytes;

public class ItemDataTest {
    private IMocksControl control;
    private BigInteger value, volume;
    private Bytes customData;
    private ItemData service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        value = BigInteger.valueOf(6689892L);
        volume = BigInteger.valueOf(115L);
        customData = new Bytes(15);
        ThreadLocalRandom.current().nextBytes(customData.getSource());
        service = new ItemData(value, 3, volume, 5, customData);
    }
    
    @Test
    public void testGetters() {
        assertSame(value, service.value());
        assertEquals(3, service.decimals());
        assertSame(volume, service.volume());
        assertEquals(5, service.volumeDecimals());
        assertSame(customData, service.customData());
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
        BigInteger value1 = BigInteger.valueOf(6689892L),
                value2 = BigInteger.valueOf(1111111L),
                volume1 = BigInteger.valueOf(115L),
                volume2 = BigInteger.valueOf(222L);
        Bytes customData1 = new Bytes(customData.copyBytes());
        Bytes customData2 = new Bytes(16);
        ThreadLocalRandom.current().nextBytes(customData2.getSource());
        assertTrue(service.equals(new ItemData(value1, 3, volume1, 5, customData1)));
        assertFalse(service.equals(new ItemData(value2, 3, volume1, 5, customData1)));
        assertFalse(service.equals(new ItemData(value1, 4, volume1, 5, customData1)));
        assertFalse(service.equals(new ItemData(value1, 3, volume2, 5, customData1)));
        assertFalse(service.equals(new ItemData(value1, 3, volume1, 6, customData1)));
        assertFalse(service.equals(new ItemData(value1, 3, volume1, 5, customData2)));
        assertFalse(service.equals(new ItemData(value2, 4, volume2, 6, customData2)));
    }
    
    @Test
    public void testEquals_ShouldBeAbleToCompareWithAnyImplementation() {
        IItemData itemMock1 = control.createMock(IItemData.class);
        expect(itemMock1.value()).andStubReturn(BigInteger.valueOf(6689892L));
        expect(itemMock1.decimals()).andStubReturn(3);
        expect(itemMock1.volume()).andStubReturn(BigInteger.valueOf(115L));
        expect(itemMock1.volumeDecimals()).andStubReturn(5);
        expect(itemMock1.customData()).andStubReturn(new Bytes(customData.copyBytes()));
        IItemData itemMock2 = control.createMock(IItemData.class);
        expect(itemMock2.value()).andStubReturn(BigInteger.valueOf(7789912L));
        expect(itemMock2.decimals()).andStubReturn(8);
        expect(itemMock2.volume()).andStubReturn(BigInteger.valueOf(728990L));
        expect(itemMock2.volumeDecimals()).andStubReturn(1);
        expect(itemMock2.customData()).andStubReturn(null);
        control.replay();
        
        assertTrue(service.equals(itemMock1));
        assertFalse(service.equals(itemMock2));
        
        control.verify();
    }
    
}
