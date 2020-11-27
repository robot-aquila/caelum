package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
    
    @Ignore
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        fail();
    }
    
    @Ignore
    @Test
    public void testVolumeDecimals_CanStoreNumberOfDecimalsInHeader() {
        fail();
    }
    
    @Ignore
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        fail();
    }
    
    @Ignore
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        fail();
    }

}
