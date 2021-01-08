package ru.prolib.caelum.lib.data.pk1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1ItemHeaderWrpTest {
    private byte[] bytes;
    private Pk1ItemHeaderWrp service;
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
        service = new Pk1ItemHeaderWrp(bytes = new byte[bufferSize], startOffset, recordSize);
        rnd.nextBytes(bytes);
    }
    
    void createService(int bufferSize, int startOffset) {
        createService(bufferSize, startOffset, bufferSize - startOffset);
    }
    
    @Test
    public void testIsA_IPk1ItemHeader() {
        createService(1, 0);
        assertThat(service, instanceOf(IPk1ItemHeader.class));
        assertThat(service, instanceOf(IPk1Header.class));
    }
    
    @Test
    public void testRecordSize() {
        createService(240, 26, 94);
        
        assertEquals(94, service.recordSize());
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
    public void testCanStoreSizesInHeader() {
        createService(115, 25);
        
        bytes[25] = ByteUtils.boolToBit(bytes[25], false, 1);
        assertTrue(service.canStoreSizesInHeader());
        bytes[25] = ByteUtils.boolToBit(bytes[25], true, 1);
        assertFalse(service.canStoreSizesInHeader());
    }
    
    @Test
    public void testDecimals_CanStoreNumberOfDecimalsInHeader() {
        createService(260, 54);
        
        bytes[54] = ByteUtils.boolToBit(bytes[54], false, 0);
        bytes[54] = ByteUtils.intToF3b(bytes[54], 4, 2);
        assertEquals(4, service.decimals());
        bytes[54] = ByteUtils.intToF3b(bytes[54], 6, 2);
        assertEquals(6, service.decimals());
        bytes[54] = ByteUtils.intToF3b(bytes[54], 0, 2);
        assertEquals(0, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreSizesInHeader() {
        createService(117, 32);
        
        bytes[32] = ByteUtils.boolToBit(bytes[32], false, 1);
        bytes[32] = ByteUtils.boolToBit(bytes[32], true, 0);
        bytes[32] = ByteUtils.intToF3b(bytes[32], 3, 2); // decimals size = 4 bytes
        // byte #33 reserved for Value-Volume params, size section is not exist
        // so the decimals section is starting from byte #34
        bytes[34] = (byte)0x15;
        bytes[35] = (byte)0xFE;
        bytes[36] = (byte)0xFF;
        bytes[37] = (byte)0x0E;
        
        assertEquals(0x15FEFF0E, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader() {
        createService(208, 15);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], true, 1);
        bytes[15] = ByteUtils.boolToBit(bytes[15], true, 0);
        bytes[15] = ByteUtils.sizeToF3b(bytes[15], 3, 2); // decimals size = 3 bytes
        bytes[16] = ByteUtils.boolToBit(bytes[16], true, 0); // has volume
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 5, 1); // length of volume size = 5 bytes
        bytes[16] = ByteUtils.boolToBit(bytes[16], true, 4); // has value
        bytes[16] = ByteUtils.sizeToF3b(bytes[16], 2, 5); // length of value size = 2 bytes
        // length of section of sizes is 5 + 2 = 7 bytes
        // so the section of decimals starts from byte #24
        bytes[24] = (byte)0x24;
        bytes[25] = (byte)0xD0;
        bytes[26] = (byte)0xEA;
        
        assertEquals(0x24D0EA, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader_HasNoValue() {
        createService(118, 26);
        
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 1);
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 0);
        bytes[26] = ByteUtils.sizeToF3b(bytes[26], 4, 2); // cannot be > 4
        bytes[27] = ByteUtils.boolToBit(bytes[27], true, 0); // has volume
        bytes[27] = ByteUtils.sizeToF3b(bytes[27], 4, 1); // length of volume size = 4 bytes
        bytes[27] = ByteUtils.boolToBit(bytes[27], false, 4); // has no value
        // keep length of value size unchanged to make this case detectable
        bytes[32] = (byte)0xFE;
        bytes[33] = (byte)0xAA;
        bytes[34] = (byte)0x00;
        bytes[35] = (byte)0x00;
        
        assertEquals(0xFEAA0000, service.decimals());
    }
    
    @Test
    public void testDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader_HasNoVolume() {
        createService(250, 116);
        
        bytes[116] = ByteUtils.boolToBit(bytes[116], true, 1);
        bytes[116] = ByteUtils.boolToBit(bytes[116], true, 0);
        bytes[116] = ByteUtils.sizeToF3b(bytes[116], 2, 2);
        bytes[117] = ByteUtils.boolToBit(bytes[117], false, 0); // has no volume
        // keep length of volume size unchanged to make this case detectable
        bytes[117] = ByteUtils.boolToBit(bytes[117], true, 4); // has value
        bytes[117] = ByteUtils.sizeToF3b(bytes[117], 3, 5); // length of value size = 3 bytes
        bytes[121] = (byte)0x0E;
        bytes[122] = (byte)0xDC;
        
        assertEquals(0x0EDC, service.decimals());
    }
    
    @Test
    public void testVolumeDecimals_CanStoreNumberOfDecimalsInHeader() {
        createService(112, 45);
        
        bytes[45] = ByteUtils.boolToBit(bytes[45], false, 0);
        bytes[45] = ByteUtils.intToF3b(bytes[45], 1, 5);
        assertEquals(1, service.volumeDecimals());
        bytes[45] = ByteUtils.intToF3b(bytes[45], 4, 5);
        assertEquals(4, service.volumeDecimals());
        bytes[45] = ByteUtils.intToF3b(bytes[45], 0, 5);
        assertEquals(0, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanStoreSizesInHeader() {
        createService(111, 15);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], true, 0);
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 1);
        bytes[15] = ByteUtils.sizeToF3b(bytes[15], 3, 2); // decimals size = 3 bytes
        bytes[15] = ByteUtils.sizeToF3b(bytes[15], 2, 5); // volume decimals size = 1 byte
        // skip 3 bytes of decimals size
        bytes[20] = (byte)0x19; // volume decimals byte#1
        bytes[21] = (byte)0xF7; // volume decimals byte#0
        
        assertEquals(0x19F7, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader() {
        createService(200, 20);
        
        bytes[20] = ByteUtils.boolToBit(bytes[20], true, 0);
        bytes[20] = ByteUtils.boolToBit(bytes[20], true, 1);
        bytes[20] = ByteUtils.sizeToF3b(bytes[20], 4, 2); // decimals size = 4 bytes
        bytes[20] = ByteUtils.sizeToF3b(bytes[20], 2, 5); // volume decimals size = 2 bytes
        bytes[21] = ByteUtils.boolToBit(bytes[21], true, 0); // has volume
        bytes[21] = ByteUtils.sizeToF3b(bytes[21], 3, 1); // volume size length = 3 bytes
        bytes[21] = ByteUtils.boolToBit(bytes[21], true, 4); // has value
        bytes[21] = ByteUtils.sizeToF3b(bytes[21], 2, 5); // value size length = 2 bytes
        // skip 5 bytes of sizes section, so 27 is next
        // skip 4 bytes of decimals size, so 31 is next
        bytes[31] = (byte)0x5F;
        bytes[32] = (byte)0xAE;
        
        assertEquals(0x5FAE, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader_HasNoValue() {
        createService(345, 112);
        
        bytes[112] = ByteUtils.boolToBit(bytes[112], true, 0);
        bytes[112] = ByteUtils.boolToBit(bytes[112], true, 1);
        bytes[112] = ByteUtils.sizeToF3b(bytes[112], 3, 2); // decimals size = 3 bytes
        bytes[112] = ByteUtils.sizeToF3b(bytes[112], 1, 5); // volume decimal size = 1 byte
        bytes[113] = ByteUtils.boolToBit(bytes[113], true, 0); // has volume
        bytes[113] = ByteUtils.sizeToF3b(bytes[113], 4, 1); // volume size length = 4 bytes
        bytes[113] = ByteUtils.boolToBit(bytes[113], false, 4); // has no value
        // skip 4 byte of sizes section, so 118 is next
        // skip 3 bytes of decimals sizes, so 121 is next
        bytes[121] = (byte)0x35;
        
        assertEquals(0x35, service.volumeDecimals());
    }
    
    @Test
    public void testVolumeDecimals_CanNotStoreNumberOfDecimalsInHeader_CanNotStoreSizesInHeader_HasNoVolume() {
        createService(762, 201);
        
        bytes[201] = ByteUtils.boolToBit(bytes[201], true, 0);
        bytes[201] = ByteUtils.boolToBit(bytes[201], true, 1);
        bytes[201] = ByteUtils.sizeToF3b(bytes[201], 4, 2); // decimals size = 4 bytes
        bytes[201] = ByteUtils.sizeToF3b(bytes[201], 2, 5); // volume decimal size = 2 bytes
        bytes[202] = ByteUtils.boolToBit(bytes[202], false, 0); // has no volume
        bytes[202] = ByteUtils.boolToBit(bytes[202], true, 4); // has value
        bytes[202] = ByteUtils.sizeToF3b(bytes[202], 2, 5); // volume size length = 2 bytes
        // skip 2 bytes of sizes section, so 205 is next
        // skip 4 bytes of decimal sizes, so 209 is next
        bytes[209] = (byte)0x75;
        bytes[210] = (byte)0x3C;
        
        assertEquals(0x753C, service.volumeDecimals());
    }
    
    @Test
    public void testIsValuePresent() {
        createService(78, 12);
        
        bytes[13] = ByteUtils.boolToBit(bytes[13], false, 4);
        assertFalse(service.isValuePresent());
        bytes[13] = ByteUtils.boolToBit(bytes[13], true, 4);
        assertTrue(service.isValuePresent());
    }
    
    @Test
    public void testIsVolumePresent() {
        createService(18, 1);
        
        bytes[2] = ByteUtils.boolToBit(bytes[2], true, 0);
        assertTrue(service.isVolumePresent());
        bytes[2] = ByteUtils.boolToBit(bytes[2], false, 0);
        assertFalse(service.isVolumePresent());
    }
    
    @Test
    public void testValueSize_HasNoValue() {
        createService(100, 25);
        
        bytes[26] = ByteUtils.boolToBit(bytes[26], false, 4);
        
        assertEquals(0, service.valueSize());
    }
    
    @Test
    public void testValueSize_CanStoreSizesInHeader() {
        createService(100, 20);
        
        bytes[20] = ByteUtils.boolToBit(bytes[20], false, 1);
        bytes[21] = ByteUtils.boolToBit(bytes[21], true, 4);
        bytes[21] = ByteUtils.sizeToF3b(bytes[21], 6, 5);
        
        assertEquals(6, service.valueSize());
    }
    
    @Test
    public void testValueSize_CanNotStoreSizesInHeader() {
        createService(124, 50);
        
        bytes[50] = ByteUtils.boolToBit(bytes[50], true, 1);
        bytes[51] = ByteUtils.boolToBit(bytes[51], true, 4);
        bytes[51] = ByteUtils.sizeToF3b(bytes[51], 4, 5);
        bytes[52] = (byte)0x0E;
        bytes[53] = (byte)0xD9;
        bytes[54] = (byte)0xAB;
        bytes[55] = (byte)0xC0;
        
        assertEquals(0x0ED9ABC0, service.valueSize());
    }
    
    @Test
    public void testVolumeSize_HasNoVolume() {
        createService(180, 35);
        
        bytes[36] = ByteUtils.boolToBit(bytes[36], false, 0); // has no volume
        
        assertEquals(0, service.volumeSize());
    }
    
    @Test
    public void testVolumeSize_CanStoreSizesInHeader() {
        createService(175, 13);
        
        bytes[13] = ByteUtils.boolToBit(bytes[13], false, 1);
        bytes[14] = ByteUtils.boolToBit(bytes[14], true, 0);
        bytes[14] = ByteUtils.sizeToF3b(bytes[14], 5, 1);
        
        assertEquals(5, service.volumeSize());
    }
    
    @Test
    public void testVolumeSize_CanNotStoreSizesInHeader_HasNoValue() {
        createService(150, 26);
        
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 1);
        bytes[27] = ByteUtils.boolToBit(bytes[27], true, 0); // has volume
        bytes[27] = ByteUtils.sizeToF3b(bytes[27], 3, 1); // volume size length is 3 bytes
        bytes[27] = ByteUtils.boolToBit(bytes[27], false, 4); // has no value
        bytes[28] = (byte)0x26;
        bytes[29] = (byte)0xFA;
        bytes[30] = (byte)0xB3;
        
        assertEquals(0x26FAB3, service.volumeSize());
    }
    
    @Test
    public void testVolumeSize_CanNotStoreSizesInHeader_HasValue() {
        createService(172, 50);
        
        bytes[50] = ByteUtils.boolToBit(bytes[50], true, 1);
        bytes[51] = ByteUtils.boolToBit(bytes[51], true, 0); // has volume
        bytes[51] = ByteUtils.sizeToF3b(bytes[51], 2, 1); // volume size length is 2 bytes
        bytes[51] = ByteUtils.boolToBit(bytes[51], true, 4); // has value
        bytes[51] = ByteUtils.sizeToF3b(bytes[51], 4, 5); // value size length is 4 bytes
        bytes[56] = (byte)0x1E;
        bytes[57] = (byte)0x3A;
        
        assertEquals(0x1E3A, service.volumeSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanStoreSizesInHeader() {
        createService(200, 15);
        
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 0); // has no decimals section in header
        bytes[15] = ByteUtils.boolToBit(bytes[15], false, 1); // has no sizes section in header
        
        assertEquals(2, service.headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanStoreSizesInHeader() {
        createService(175, 90);
        
        bytes[90] = ByteUtils.boolToBit(bytes[90], true, 0); // has decimals section in header
        bytes[90] = ByteUtils.boolToBit(bytes[90], false, 1); // has no sizes section in header
        bytes[90] = ByteUtils.sizeToF3b(bytes[90], 3, 2); // decimals size = 3
        bytes[90] = ByteUtils.sizeToF3b(bytes[90], 4, 5); // volume decimals size = 4
        
        assertEquals(9, service.headerSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreSizesInHeader() {
        createService(250, 100);
        
        bytes[100] = ByteUtils.boolToBit(bytes[100], false, 0); // has no decimals section in header
        bytes[100] = ByteUtils.boolToBit(bytes[100], true, 1); // has sizes section in header
        bytes[101] = ByteUtils.boolToBit(bytes[101], true, 0); // has volume
        bytes[101] = ByteUtils.sizeToF3b(bytes[101], 2, 1); // volume size length is 2 bytes
        bytes[101] = ByteUtils.boolToBit(bytes[101], true, 4); // has value
        bytes[101] = ByteUtils.sizeToF3b(bytes[101], 1, 5); // value size length is 1 byte
        
        assertEquals(5, service.headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreSizesInHeader() {
        createService(1000, 250);
        
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 0); // has decimals section in header
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 1); // has sizes section in header
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 2, 2); // decimals size = 2
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 1, 5); // volume decimals size = 1
        bytes[251] = ByteUtils.boolToBit(bytes[251], true, 0); // has volume
        bytes[251] = ByteUtils.sizeToF3b(bytes[251], 4, 1); // volume size length is 4 bytes
        bytes[251] = ByteUtils.boolToBit(bytes[251], true, 4); // has value
        bytes[251] = ByteUtils.sizeToF3b(bytes[251], 2, 5); // value size length is 2 bytes
        
        assertEquals(11, service.headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreSizesInHeader_HasNoValue() {
        createService(1000, 250);
        
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 0); // has decimals section in header
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 1); // has sizes section in header
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 2, 2); // decimals size = 2
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 1, 5); // volume decimals size = 1
        bytes[251] = ByteUtils.boolToBit(bytes[251], true, 0); // has volume
        bytes[251] = ByteUtils.sizeToF3b(bytes[251], 4, 1); // volume size length is 4 bytes
        bytes[251] = ByteUtils.boolToBit(bytes[251], false, 4); // has no value
        
        assertEquals(9, service.headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreSizesInHeader_HasNoVolume() {
        createService(1000, 250);
        
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 0); // has decimals section in header
        bytes[250] = ByteUtils.boolToBit(bytes[250], true, 1); // has sizes section in header
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 2, 2); // decimals size = 2
        bytes[250] = ByteUtils.sizeToF3b(bytes[250], 1, 5); // volume decimals size = 1
        bytes[251] = ByteUtils.boolToBit(bytes[251], false, 0); // has no volume
        bytes[251] = ByteUtils.boolToBit(bytes[251], true, 4); // has value
        bytes[251] = ByteUtils.sizeToF3b(bytes[251], 2, 5); // value size length is 2 bytes
        
        assertEquals(7, service.headerSize());
    }
    
    @Test
    public void testCustomDataSize() {
        createService(100, 25, 50);
        
        bytes[25] = ByteUtils.boolToBit(bytes[25], false, 0);
        bytes[25] = ByteUtils.boolToBit(bytes[25], false, 1);
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 0);
        bytes[26] = ByteUtils.sizeToF3b(bytes[26], 3, 1);
        bytes[26] = ByteUtils.boolToBit(bytes[26], true, 4);
        bytes[26] = ByteUtils.sizeToF3b(bytes[26], 2, 5);
        
        // header size is 2
        // value size is 2
        // volume size is 3
        assertEquals(43, service.customDataSize());
    }
    
    @Test
    public void testCustomDataSize_HasNoValue() {
        createService(248, 17, 50);
        
        bytes[17] = ByteUtils.boolToBit(bytes[17], false, 0);
        bytes[17] = ByteUtils.boolToBit(bytes[17], false, 1);
        bytes[18] = ByteUtils.boolToBit(bytes[18], true, 0);
        bytes[18] = ByteUtils.sizeToF3b(bytes[18], 3, 1);
        bytes[18] = ByteUtils.boolToBit(bytes[18], false, 4);
        
        assertEquals(45, service.customDataSize());
    }
    
    @Test
    public void testCustomDataSize_HasNoVolume() {
        createService(750, 180, 60);
        
        bytes[180] = ByteUtils.boolToBit(bytes[180], false, 0);
        bytes[180] = ByteUtils.boolToBit(bytes[180], false, 1);
        bytes[181] = ByteUtils.boolToBit(bytes[181], false, 0);
        bytes[181] = ByteUtils.boolToBit(bytes[181], true, 4);
        bytes[181] = ByteUtils.sizeToF3b(bytes[181], 2, 5);
        
        assertEquals(56, service.customDataSize());
    }

}
