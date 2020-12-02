package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import java.math.BigInteger;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class Pk1TupleDataTest {
    private IMocksControl control;
    private IPk1TupleHeader headerMock;
    private byte[] bytes;
    private Pk1TupleData service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        headerMock = control.createMock(IPk1TupleHeader.class);
        createService();
    }
    
    private void createService(int bufferSize, int startOffset) {
        service = new Pk1TupleData(headerMock, new Bytes(
                bytes = new byte[bufferSize],
                startOffset,
                bufferSize - startOffset
            ));
    }
    
    private void createService() {
        createService(100, 0);
    }
    
    @Test
    public void testOpen() {
        createService(256, 12);
        expect(headerMock.headerSize()).andReturn(8);
        expect(headerMock.openSize()).andReturn(7);
        System.arraycopy(ByteUtils.hexStringToByteArr("1B72EAD8D7CF72"), 0, bytes, 20, 7);
        control.replay();
        
        assertEquals(BigInteger.valueOf(7726177357123442L), service.open());
        
        control.verify();
    }
    
    @Test
    public void testHigh_Absolute() {
        createService(512, 46);
        expect(headerMock.headerSize()).andReturn(4);
        expect(headerMock.openSize()).andReturn(9);
        expect(headerMock.highSize()).andReturn(6);
        expect(headerMock.isHighRelative()).andReturn(false);
        System.arraycopy(ByteUtils.hexStringToByteArr("0912F38CF4F9"), 0, bytes, 59, 6);
        control.replay();
        
        assertEquals(BigInteger.valueOf(9977000162553L), service.high());
        
        control.verify();
    }
    
    @Test
    public void testHigh_Relative() {
        createService(100, 20);
        expect(headerMock.headerSize()).andStubReturn(4);
        expect(headerMock.openSize()).andStubReturn(3);
        expect(headerMock.highSize()).andReturn(3);
        expect(headerMock.isHighRelative()).andReturn(true);
        System.arraycopy(ByteUtils.hexStringToByteArr("00B08D"), 0, bytes, 24, 3); // open 45197 = B08D
        System.arraycopy(ByteUtils.hexStringToByteArr("014A1E"), 0, bytes, 27, 3); // high 84510 = 014A1E
        control.replay();
        
        assertEquals(BigInteger.valueOf(129707L), service.high());
        
        control.verify();
    }
    
    @Test
    public void testLow_Absolute() {
        createService(117, 28);
        expect(headerMock.headerSize()).andReturn(15);
        expect(headerMock.openSize()).andReturn(4);
        expect(headerMock.highSize()).andReturn(15);
        expect(headerMock.lowSize()).andReturn(8);
        expect(headerMock.isLowRelative()).andReturn(false);
        System.arraycopy(ByteUtils.hexStringToByteArr("058DC98C0AF53F18"), 0, bytes, 62, 8);
        control.replay();
        
        assertEquals(BigInteger.valueOf(400197545222291224L), service.low());
        
        control.verify();
    }
    
    @Test
    public void testLow_Relative() {
        createService(115, 25);
        expect(headerMock.headerSize()).andStubReturn(23);
        expect(headerMock.openSize()).andStubReturn(2);
        expect(headerMock.highSize()).andReturn(5);
        expect(headerMock.lowSize()).andReturn(2);
        expect(headerMock.isLowRelative()).andReturn(true);
        System.arraycopy(ByteUtils.hexStringToByteArr("00B0"), 0, bytes, 48, 2); // open 176 = B0
        System.arraycopy(ByteUtils.hexStringToByteArr("FDD1"), 0, bytes, 55, 2); // low -559 = FDD1
        control.replay();
        
        assertEquals(BigInteger.valueOf(-383L), service.low());
        
        control.verify();
    }
    
    @Test
    public void testClose_Absolute() {
        createService(290, 73);
        expect(headerMock.headerSize()).andReturn(3);
        expect(headerMock.openSize()).andReturn(20);
        expect(headerMock.highSize()).andReturn(7);
        expect(headerMock.lowSize()).andReturn(15);
        expect(headerMock.closeSize()).andReturn(14);
        expect(headerMock.isCloseRelative()).andReturn(false);
        System.arraycopy(ByteUtils.hexStringToByteArr("058DC98C0AF53F1800461927FFBA"), 0, bytes, 118, 14);
        control.replay();
        
        BigInteger expected = new BigInteger(ByteUtils.hexStringToByteArr("058DC98C0AF53F1800461927FFBA"));
        assertEquals(expected, service.close());
        
        control.verify();
    }
    
    @Test
    public void testClose_Relative() {
        createService(100, 0);
        expect(headerMock.headerSize()).andStubReturn(12);
        expect(headerMock.openSize()).andStubReturn(1);
        expect(headerMock.highSize()).andReturn(4);
        expect(headerMock.lowSize()).andReturn(5);
        expect(headerMock.closeSize()).andReturn(3);
        expect(headerMock.isCloseRelative()).andReturn(true);
        System.arraycopy(ByteUtils.hexStringToByteArr("EA"), 0, bytes, 12, 1); // open -22 = EA
        System.arraycopy(ByteUtils.hexStringToByteArr("F2F7A0"), 0, bytes, 22, 3); // close -854112 = F2F7A0
        control.replay();
        
        assertEquals(BigInteger.valueOf(-854134L), service.close());
        
        control.verify();
    }
    
    @Test
    public void testDecimals() {
        expect(headerMock.decimals()).andReturn(15);
        control.replay();
        
        assertEquals(15, service.decimals());
        
        control.verify();
    }
    
    @Test
    public void testVolume() {
        createService(69, 5);
        expect(headerMock.volumeSize()).andReturn(8);
        expect(headerMock.recordSize()).andReturn(64);
        System.arraycopy(ByteUtils.hexStringToByteArr("E438EFEEB24DEF87"), 0, bytes, 61, 8);
        control.replay();
        
        assertEquals(BigInteger.valueOf(-2001586225893478521L), service.volume());
        
        control.verify();
    }
    
    @Test
    public void testVolumeDecimals() {
        expect(headerMock.volumeDecimals()).andReturn(3);
        control.replay();
        
        assertEquals(3, service.volumeDecimals());
        
        control.verify();
    }
    
}
