package ru.prolib.caelum.lib.data.pk1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1TupleHeaderWrpTest {
    private byte[] bytes;
    private Pk1TupleHeaderWrp service;
    private ThreadLocalRandom rnd;
    
    @Before
    public void setUp() throws Exception {
        rnd = ThreadLocalRandom.current();
    }
    
    @After
    public void tearDown() {
        bytes = null;
        service = null;
    }
    
    void createService(int bufferSize, int startOffset, int recordSize) {
        service = new Pk1TupleHeaderWrp(bytes = new byte[bufferSize], startOffset, recordSize);
        rnd.nextBytes(bytes);
    }
    
    void createService(int bufferSize, int startOffset) {
        createService(bufferSize, startOffset, bufferSize - startOffset);
    }
    
    @Test
    public void testIsA_IPk1TupleHeader() {
        createService(1, 0);
        assertThat(service, instanceOf(IPk1TupleHeader.class));
    }
    
    @Test
    public void testRecordSize() {
        createService(100, 15, 24);
        
        assertEquals(24, service.recordSize());
    }
    
    @Test
    public void testCanStoreNumberOfDecimalsInHeader() {
        createService(10, 5);
        
        bytes[5] = ByteUtils.boolToBit(bytes[5], false, 0);
        assertTrue(service.canStoreNumberOfDecimalsInHeader());
        bytes[5] = ByteUtils.boolToBit(bytes[5], true, 0);
        assertFalse(service.canStoreNumberOfDecimalsInHeader());
    }
    
    @Test
    public void testCanStoreOhlcSizesInHeader() {
        createService(20, 10);
        
        bytes[10] = ByteUtils.boolToBit(bytes[10], false, 1);
        assertTrue(service.canStoreOhlcSizesInHeader());
        bytes[10] = ByteUtils.boolToBit(bytes[10], true, 1);
        assertFalse(service.canStoreOhlcSizesInHeader());
    }
    
    @Test
    public void testDecimals_CanStoreNumberOfDecimalsInHeader() {
        createService(32, 16);
        
        bytes[16] = ByteUtils.boolToBit(bytes[16], false, 0);
        bytes[16] = ByteUtils.intToF3b(bytes[16], 5, 2);
        assertEquals(5, service.decimals());
        bytes[16] = ByteUtils.intToF3b(bytes[16], 0, 2);
        assertEquals(0, service.decimals());
        bytes[16] = ByteUtils.intToF3b(bytes[16], 7, 2);
        assertEquals(7, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService(48, 8);
        
        bytes[8] = ByteUtils.boolToBit(bytes[8], true,  0); // decimals are in section
        bytes[8] = ByteUtils.boolToBit(bytes[8], false, 1); // OHLC sizes in header
        bytes[8] = ByteUtils.intToF3b(bytes[8], 1, 2); // decimals size = 2 bytes
        // Bytes 9 and 10 are reserved for OHLC data. OHLC sizes section is not exist.
        // So the decimals section is starting from byte 11.
        bytes[11] = (byte)0x01;
        bytes[12] = (byte)0xFE;
        
        assertEquals(510, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService(52, 8);
        
        bytes[8] = ByteUtils.boolToBit(bytes[8], true, 0); // decimals are in section
        bytes[8] = ByteUtils.boolToBit(bytes[8], true, 1); // OHLC sizes are in section
        bytes[8] = ByteUtils.intToF3b(bytes[8], 2, 2); // decimals size = 3 bytes
        bytes[9] = (byte) (ByteUtils.sizeToF3b(4, 1) | ByteUtils.sizeToF3b(2, 5));
        bytes[10] = (byte) (ByteUtils.sizeToF3b(1, 1) | ByteUtils.sizeToF3b(3, 5));
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
        
        bytes[2] = ByteUtils.boolToBit(bytes[2], false, 0);
        bytes[2] = ByteUtils.intToF3b(bytes[2], 4, 5);
        assertEquals(4, service.volumeDecimals());
        bytes[2] = ByteUtils.intToF3b(bytes[2], 0, 5);
        assertEquals(0, service.volumeDecimals());
        bytes[2] = ByteUtils.intToF3b(bytes[2], 7, 5);
        assertEquals(7, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService(27, 3);
        
        bytes[3] = ByteUtils.boolToBit(bytes[3], true , 0);
        bytes[3] = ByteUtils.boolToBit(bytes[3], false, 1);
        bytes[3] = ByteUtils.intToF3b(bytes[3], 1, 2); // we need to know the length of "decimals" - 2 bytes
        bytes[3] = ByteUtils.intToF3b(bytes[3], 3, 5); // 4 bytes of volumeDecimals
        bytes[ 8] = (byte)0x04;
        bytes[ 9] = (byte)0x12;
        bytes[10] = (byte)0x5E;
        bytes[11] = (byte)0x45;
        
        assertEquals(68312645, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService(115, 26);
        
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 0);
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 1);
        bytes[26] = ByteUtils.intToF3b(bytes[26], 3, 2); // we need to know the length of "decimals" - 4 bytes
        bytes[26] = ByteUtils.intToF3b(bytes[26], 0, 5); // 1 byte of volumeDecimals
        bytes[27] = (byte) (ByteUtils.sizeToF3b(2, 1) | ByteUtils.sizeToF3b(3, 5));
        bytes[28] = (byte) (ByteUtils.sizeToF3b(4, 1) | ByteUtils.sizeToF3b(2, 5));
        // OHLC sizes section exists and has 11 bytes.
        // So the decimals section is starting from byte 40, + 4 bytes of "decimals" field is the POI
        bytes[44] = (byte)0x73;
        
        assertEquals(115, service.volumeDecimals());
    }
    
    @Test
    public void testOpenSize_CanStoreOhlcSizesInHeader() {
        createService(260, 100);
        
        bytes[100] = ByteUtils.boolToBit(bytes[100], false, 1);
        bytes[101] = ByteUtils.sizeToF3b(bytes[101], 3, 5);
        
        assertEquals(3, service.openSize());
    }
    
    @Test
    public void testOpenSize_CanNotStoreOhlcSizesInHeader() {
        createService(118, 4);
        
        bytes[4] = ByteUtils.boolToBit(bytes[4], true, 1);
        bytes[5] = ByteUtils.sizeToF3b(bytes[5], 3, 5);
        bytes[7] = (byte)0x00;
        bytes[8] = (byte)0xAA;
        bytes[9] = (byte)0xF5;
        
        assertEquals(43765, service.openSize());
    }
    
    @Test
    public void testIsHighRelative() {
        createService(112, 57);
        
        bytes[58] = ByteUtils.boolToBit(bytes[58], false, 0);
        assertFalse(service.isHighRelative());
        bytes[58] = ByteUtils.boolToBit(bytes[58], true, 0);
        assertTrue(service.isHighRelative());
    }
    
    @Test
    public void testHighSize_CanStoreOhlcSizesInHeader() {
        createService(75, 14);
        
        bytes[14] = ByteUtils.boolToBit(bytes[14], false, 1);
        bytes[15] = ByteUtils.sizeToF3b(bytes[15], 5, 1);
        
        assertEquals(5, service.highSize());
    }
    
    @Test
    public void testHighSize_CanNotStoreOhlcSizesInHeader() {
        createService(18, 1);
        
        bytes[1] = ByteUtils.boolToBit(bytes[1], true, 1);
        bytes[2] = ByteUtils.sizeToF3b(bytes[2], 4, 5); // we need to know the length of open size
        bytes[2] = ByteUtils.sizeToF3b(bytes[2], 2, 1);
        bytes[8] = (byte)0x15;
        bytes[9] = (byte)0xFE;
        
        assertEquals(5630, service.highSize());
    }
    
    @Test
    public void testIsLowRelative() {
        createService(102, 13);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 4);
        assertFalse(service.isLowRelative());
        bytes[15] = ByteUtils.boolToBit(bytes[15], true, 4);
        assertTrue(service.isLowRelative());
    }
    
    @Test
    public void testLowSize_CanStoreOhlcSizesInHeader() {
        createService(26, 9);
        
        bytes[ 9] = ByteUtils.boolToBit(bytes[9], false, 1);
        bytes[11] = ByteUtils.sizeToF3b(bytes[11], 2, 5);
        
        assertEquals(2, service.lowSize());
    }
    
    @Test
    public void testLowSize_CanNotStoreOhlcSizesInHeader() {
        createService(78, 3);
        
        bytes[ 3] = ByteUtils.boolToBit(bytes[3], true, 1);
        bytes[ 4] = ByteUtils.sizeToF3b(bytes[4], 2, 5); // we need to know the length of open size
        bytes[ 4] = ByteUtils.sizeToF3b(bytes[4], 5, 1); // ... and of high size
        bytes[ 5] = ByteUtils.sizeToF3b(bytes[5], 4, 5);
        // low size offset is: 3 + 3 + 2 + 5 = 13
        bytes[13] = (byte)0x24;
        bytes[14] = (byte)0x00;
        bytes[15] = (byte)0x1F;
        bytes[16] = (byte)0xFB;
        
        assertEquals(603987963, service.lowSize());
    }
    
    @Test
    public void testIsCloseRelative() {
        createService(115, 26);
        
        bytes[28] = ByteUtils.boolToBit(bytes[28], false, 0);
        assertFalse(service.isCloseRelative());
        bytes[28] = ByteUtils.boolToBit(bytes[28], true, 0);
        assertTrue(service.isCloseRelative());
    }
    
    @Test
    public void testCloseSize_CanStoreOhlcSizesInHeader() {
        createService(100, 15);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 1);
        bytes[17] = ByteUtils.sizeToF3b(bytes[17], 3, 1);
        
        assertEquals(3, service.closeSize());
    }
    
    @Test
    public void testCloseSize_CanNotStoreOhlcSizesInHeader() {
        createService(180, 54);
        
        bytes[54] = ByteUtils.boolToBit(bytes[54], true, 1);
        bytes[55] = ByteUtils.sizeToF3b(bytes[55], 1, 5); // we need to know the length of open size
        bytes[55] = ByteUtils.sizeToF3b(bytes[55], 4, 1); // ... and of high size
        bytes[56] = ByteUtils.sizeToF3b(bytes[56], 3, 5); // ... and of low size
        bytes[56] = ByteUtils.sizeToF3b(bytes[56], 2, 1);
        // close size offset is: 54 + 3 + 1 + 4 + 3 = 65;
        bytes[65] = (byte)0x27;
        bytes[66] = (byte)0x00;
        
        assertEquals(9984, service.closeSize());
    }
    
    private void createService_296_15_44() {
        createService(296, 15, 44);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 0);
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 1);
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 2, 5);
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 1, 1);
        bytes[17] = ByteUtils.sizeToF3b(bytes[17], 4, 5);
        bytes[17] = ByteUtils.sizeToF3b(bytes[17], 3, 1);
        // So payload is right after 3 bytes of header and it is: 3 + 2 + 1 + 4 + 3 + X where X are bytes of volume
        // X = 44 - (3 + 2 + 1 + 4 + 3) = 31
    }
    
    @Test
    public void testVolumeSize_CanStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService_296_15_44();
        
        assertEquals(31, service.volumeSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService_296_15_44();
        
        assertEquals(3, service.headerSize());
    }
    
    private void createService_115_26_95() {
        createService(115, 26, 95);
        
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 0);
        bytes[26] = ByteUtils.boolToBit(bytes[26], false, 1);
        bytes[26] = ByteUtils.sizeToF3b(bytes[26], 3, 2); // we need to know the decimals size
        bytes[26] = ByteUtils.sizeToF3b(bytes[26], 2, 5); // ... and the volumeDecimals size
        bytes[27] = ByteUtils.sizeToF3b(bytes[27], 4, 5);
        bytes[27] = ByteUtils.sizeToF3b(bytes[27], 2, 1);
        bytes[28] = ByteUtils.sizeToF3b(bytes[28], 3, 5);
        bytes[28] = ByteUtils.sizeToF3b(bytes[28], 1, 1);
        // header length is 3 + 3 + 2 = 8
        // + 4 + 2 + 3 + 1 + X = 18 + X
        // X = 95 - 18 = 77
    }
    
    @Test
    public void testVolumeSize_CanNotStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService_115_26_95();
        
        assertEquals(77, service.volumeSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        createService_115_26_95();
        
        assertEquals(8, service.headerSize());
    }
    
    private void createService_209_15_58() {
        createService(209, 15, 58);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 0);
        bytes[15] = ByteUtils.boolToBit(bytes[15], true, 1);
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 4, 5);
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 2, 1);
        bytes[17] = ByteUtils.sizeToF3b(bytes[17], 3, 5);
        bytes[17] = ByteUtils.sizeToF3b(bytes[17], 1, 1);
        bytes[18] = (byte)0x00; // open size
        bytes[19] = (byte)0x00;
        bytes[20] = (byte)0x00;
        bytes[21] = (byte)0x05;
        bytes[22] = (byte)0x00; // high size
        bytes[23] = (byte)0x04;
        bytes[24] = (byte)0x00; // low size
        bytes[25] = (byte)0x00;
        bytes[26] = (byte)0x08;
        bytes[27] = (byte)0x02; // close size
        // header length is 13 bytes
        // OHLC data is 5 + 4 + 8 + 2 = 19 bytes
        // X = 58 - (13 + 19) = 26
    }
    
    @Test
    public void testVolumeSize_CanStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService_209_15_58();
        
        assertEquals(26, service.volumeSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService_209_15_58();
        
        assertEquals(13, service.headerSize());
    }
    
    private void createService_104_11_57() {
        createService(104, 11, 57);
        
        bytes[11] = ByteUtils.boolToBit(bytes[11], true, 0);
        bytes[11] = ByteUtils.boolToBit(bytes[11], true, 1);
        bytes[11] = ByteUtils.sizeToF3b(bytes[11], 3, 2);
        bytes[11] = ByteUtils.sizeToF3b(bytes[11], 2, 5);
        bytes[12] = ByteUtils.sizeToF3b(bytes[12], 4, 5);
        bytes[12] = ByteUtils.sizeToF3b(bytes[12], 2, 1);
        bytes[13] = ByteUtils.sizeToF3b(bytes[13], 3, 5);
        bytes[13] = ByteUtils.sizeToF3b(bytes[13], 1, 1);
        bytes[14] = (byte)0x00; // open size
        bytes[15] = (byte)0x00;
        bytes[16] = (byte)0x00;
        bytes[17] = (byte)0x05;
        bytes[18] = (byte)0x00; // high size
        bytes[19] = (byte)0x03;
        bytes[20] = (byte)0x00; // low size
        bytes[21] = (byte)0x00;
        bytes[22] = (byte)0x0A;
        bytes[23] = (byte)0x07; // close size
        // bytes 24-28 are for decimals, so byte 29 is the first byte of payload
        // header length = 18
        // OHLC sizes = 5 + 3 + 10 + 7 = 25
        // X = 57 - (18 + 25) = 14
    }
    
    @Test
    public void testVolumeSize_CanNotStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService_104_11_57();
        
        assertEquals(14, service.volumeSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        createService_104_11_57();
        
        assertEquals(18, service.headerSize());
    }
    
}
