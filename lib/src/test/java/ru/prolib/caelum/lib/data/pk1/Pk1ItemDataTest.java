package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import java.math.BigInteger;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class Pk1ItemDataTest {
    private IMocksControl control;
    private IPk1ItemHeader headerMock;
    private byte[] bytes;
    private Pk1ItemData service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        headerMock = control.createMock(IPk1ItemHeader.class);
        createService();
    }
    
    private void createService(int bufferSize, int startOffset, int recordSize) {
        service = new Pk1ItemData(headerMock, new Bytes(bytes = new byte[bufferSize], startOffset, recordSize));
    }
    
    private void createService(int bufferSize, int startOffset) {
        createService(bufferSize, startOffset, bufferSize - startOffset);
    }
    
    private void createService() {
        createService(100, 0);
    }
    
    @Test
    public void testValue() {
        createService(105, 12);
        expect(headerMock.isValuePresent()).andReturn(true);
        expect(headerMock.headerSize()).andReturn(10);
        expect(headerMock.valueSize()).andReturn(5);
        System.arraycopy(ByteUtils.hexStringToByteArr("1B72EAD8D7"), 0, bytes, 22, 5);
        control.replay();
        
        assertEquals(BigInteger.valueOf(0x1B72EAD8D7L), service.value());
        
        control.verify();
    }
    
    @Test
    public void testValue_HasNoValue() {
        expect(headerMock.isValuePresent()).andReturn(false);
        control.replay();
        
        assertNull(service.value());
        
        control.verify();
    }
    
    @Test
    public void testDecimals() {
        expect(headerMock.decimals()).andReturn(5);
        control.replay();
        
        assertEquals(5, service.decimals());
        
        control.verify();
    }
    
    @Test
    public void testVolume() {
        createService(235, 25);
        expect(headerMock.isVolumePresent()).andReturn(true);
        expect(headerMock.headerSize()).andReturn(12);
        expect(headerMock.valueSize()).andReturn(7);
        expect(headerMock.volumeSize()).andReturn(4);
        System.arraycopy(ByteUtils.hexStringToByteArr("0532DEA0"), 0, bytes, 44, 4);
        control.replay();
        
        assertEquals(BigInteger.valueOf(0x0532DEA0L), service.volume());
        
        control.verify();
    }
    
    @Test
    public void testVolume_HasNoVolume() {
        expect(headerMock.isVolumePresent()).andReturn(false);
        control.replay();
        
        assertEquals(BigInteger.ZERO, service.volume());
        
        control.verify();
    }
    
    @Test
    public void testVolumeDecimals() {
        expect(headerMock.volumeDecimals()).andReturn(16);
        control.replay();
        
        assertEquals(16, service.volumeDecimals());
        
        control.verify();
    }
    
    @Test
    public void testCustomData() {
        createService(100, 20, 50);
        expect(headerMock.recordSize()).andStubReturn(50);
        expect(headerMock.customDataSize()).andStubReturn(5);
        System.arraycopy(ByteUtils.hexStringToByteArr("AABBCCDDEE"), 0, bytes, 65, 5);
        control.replay();
        
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("AABBCCDDEE")), service.customData());
        
        control.verify();
    }
    
    @Test
    public void testCustomData_Case2() {
        createService(12, 0, 12);
        expect(headerMock.recordSize()).andStubReturn(12);
        expect(headerMock.customDataSize()).andStubReturn(5);
        System.arraycopy(ByteUtils.hexStringToByteArr("1020304050"), 0, bytes, 7, 5);
        control.replay();
        
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("1020304050")), service.customData());
        
        control.verify();
    }

    
    @Test
    public void testCustomData_HasNoCustomData() {
        createService();
        expect(headerMock.customDataSize()).andReturn(0);
        control.replay();
        
        assertNull(service.customData());
        
        control.verify();
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
        Bytes bytesMock1 = control.createMock(Bytes.class);
        Bytes bytesMock2 = control.createMock(Bytes.class);
        IPk1ItemHeader headerMock2 = control.createMock(IPk1ItemHeader.class);
        service = new Pk1ItemData(headerMock, bytesMock1);
        assertTrue(service.equals(new Pk1ItemData(headerMock, bytesMock1)));
        assertFalse(service.equals(new Pk1ItemData(headerMock2, bytesMock1)));
        assertFalse(service.equals(new Pk1ItemData(headerMock, bytesMock2)));
        assertFalse(service.equals(new Pk1ItemData(headerMock2, bytesMock2)));
    }

}
