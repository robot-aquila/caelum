package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1TupleHeaderWrpTest {
    private ByteUtils byteUtils;
    private byte[] bytes;
    private Pk1TupleHeaderWrp service;
    private ThreadLocalRandom rnd;
    
    @Before
    public void setUp() throws Exception {
        byteUtils = new ByteUtils();
        rnd = ThreadLocalRandom.current();
    }
    
    @After
    public void tearDown() {
        bytes = null;
        service = null;
    }
    
    void createService(int size, int startOffset) {
        service = new Pk1TupleHeaderWrp(byteUtils, bytes = new byte[size], startOffset);
        rnd.nextBytes(bytes);
    }
    
    @Test
    public void testCanStoreNumberOfDecimalsInHeader() {
        createService(10, 5);
        
        bytes[5] = byteUtils.boolToBit(bytes[5], false, 0);
        assertTrue(service.canStoreNumberOfDecimalsInHeader());
        bytes[5] = byteUtils.boolToBit(bytes[5], true, 0);
        assertFalse(service.canStoreNumberOfDecimalsInHeader());
    }
    
    @Test
    public void testCanStoreOhlcSizesInHeader() {
        createService(20, 10);
        
        bytes[10] = byteUtils.boolToBit(bytes[10], false, 1);
        assertTrue(service.canStoreOhlcSizesInHeader());
        bytes[10] = byteUtils.boolToBit(bytes[10], true, 1);
        assertFalse(service.canStoreOhlcSizesInHeader());
    }
    
    @Test
    public void testDecimals_CanStoreNumberOfDecimalsInHeader() {
        createService(32, 16);
        
        bytes[16] = byteUtils.boolToBit(bytes[16], false, 0);
        bytes[16] = byteUtils.intToF3b(bytes[16], 5, 2);
        assertEquals(5, service.decimals());
        bytes[16] = byteUtils.intToF3b(bytes[16], 0, 2);
        assertEquals(0, service.decimals());
        bytes[16] = byteUtils.intToF3b(bytes[16], 7, 2);
        assertEquals(7, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService(48, 8);
        
        bytes[8] = byteUtils.boolToBit(bytes[8], true,  0); // decimals are in section
        bytes[8] = byteUtils.boolToBit(bytes[8], false, 1); // OHLC sizes in header
        bytes[8] = byteUtils.intToF3b(bytes[8], 1, 2); // decimals size = 2 bytes
        // Bytes 9 and 10 are reserved for OHLC data. OHLC sizes section is not exist.
        // So the decimals section is starting from byte 11.
        bytes[11] = (byte)0x01;
        bytes[12] = (byte)0xFE;
        
        assertEquals(510, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService(52, 8);
        
        bytes[8] = byteUtils.boolToBit(bytes[8], true, 0); // decimals are in section
        bytes[8] = byteUtils.boolToBit(bytes[8], true, 1); // OHLC sizes are in section
        bytes[8] = byteUtils.intToF3b(bytes[8], 2, 2); // decimals size = 3 bytes
        bytes[9] = (byte) (byteUtils.sizeToF3b(4, 1) | byteUtils.sizeToF3b(2, 5));
        bytes[10] = (byte) (byteUtils.sizeToF3b(1, 1) | byteUtils.sizeToF3b(3, 5));
        // OHLC sizes section exists and has 10 bytes.
        // So the decimals section is starting from byte 21
        bytes[21] = (byte)0x02;
        bytes[22] = (byte)0x06;
        bytes[23] = (byte)0x49;
        
        assertEquals(132681, service.decimals());
    }
    
    @Test
    public void testVolumeDecimals_CanStoreNumberOfDecimalsInHeader() {
        createService(26, 2);
        
        bytes[2] = byteUtils.boolToBit(bytes[2], false, 0);
        bytes[2] = byteUtils.intToF3b(bytes[2], 4, 5);
        assertEquals(4, service.volumeDecimals());
        bytes[2] = byteUtils.intToF3b(bytes[2], 0, 5);
        assertEquals(0, service.volumeDecimals());
        bytes[2] = byteUtils.intToF3b(bytes[2], 7, 5);
        assertEquals(7, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService(27, 3);
        
        bytes[3] = byteUtils.boolToBit(bytes[3], true , 0);
        bytes[3] = byteUtils.boolToBit(bytes[3], false, 1);
        bytes[3] = byteUtils.intToF3b(bytes[3], 1, 2); // we need to know the length of "decimals" - 2 bytes
        bytes[3] = byteUtils.intToF3b(bytes[3], 3, 5); // 4 bytes of volumeDecimals
        bytes[ 8] = (byte)0x04;
        bytes[ 9] = (byte)0x12;
        bytes[10] = (byte)0x5E;
        bytes[11] = (byte)0x45;
        
        assertEquals(68312645, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService(115, 26);
        
        bytes[26] = byteUtils.boolToBit(bytes[26], true, 0);
        bytes[26] = byteUtils.boolToBit(bytes[26], true, 1);
        bytes[26] = byteUtils.intToF3b(bytes[26], 3, 2); // we need to know the length of "decimals" - 4 bytes
        bytes[26] = byteUtils.intToF3b(bytes[26], 0, 5); // 1 byte of volumeDecimals
        bytes[27] = (byte) (byteUtils.sizeToF3b(2, 1) | byteUtils.sizeToF3b(3, 5));
        bytes[28] = (byte) (byteUtils.sizeToF3b(4, 1) | byteUtils.sizeToF3b(2, 5));
        // OHLC sizes section exists and has 11 bytes.
        // So the decimals section is starting from byte 40, + 4 bytes of "decimals" field is the POI
        bytes[44] = (byte)0x73;
        
        assertEquals(115, service.volumeDecimals());
    }

}
