package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.RawTuple;

public class Pk1UtilsTest {
    private Pk1Utils service;

    @Before
    public void setUp() throws Exception {
        service = new Pk1Utils(ByteUtils.getInstance());
    }

    @Test
    public void testToTuplePk_AllOhlcComponentsAbsolute() {
        RawTuple tuple = new RawTuple(
                new Bytes(BigInteger.valueOf(17289L).toByteArray()),
                new Bytes(BigInteger.valueOf(5000009912435L).toByteArray()),
                new Bytes(BigInteger.valueOf(-98).toByteArray()),
                new Bytes(BigInteger.valueOf(-23).toByteArray()),
                4,
                new Bytes(BigInteger.valueOf(100000L).toByteArray()),
                6
            );
        
        Pk1Tuple actual = service.toTuplePk(tuple);
        
        assertEquals(new Pk1Tuple(
                new Pk1TupleHeaderBuilder()
                    .openSize(2)
                    .highRelative(false)
                    .highSize(6)
                    .lowRelative(false)
                    .lowSize(1)
                    .closeRelative(false)
                    .closeSize(1)
                    .decimals(4)
                    .volumeSize(3)
                    .volumeDecimals(6)
                    .build(),
                new Pk1TuplePayload(
                    new Bytes(BigInteger.valueOf(17289L).toByteArray()),
                    new Bytes(BigInteger.valueOf(5000009912435L).toByteArray()),
                    new Bytes(BigInteger.valueOf(-98).toByteArray()),
                    new Bytes(BigInteger.valueOf(-23).toByteArray()),
                    new Bytes(BigInteger.valueOf(100000L).toByteArray())
                )
            ), actual);
    }
    
    @Test
    public void testToTuplePk_AllOhlcComponentsRelative() {
        RawTuple tuple = new RawTuple(
                new Bytes(BigInteger.valueOf(779900071L).toByteArray()),
                new Bytes(BigInteger.valueOf(779900099L).toByteArray()),
                new Bytes(BigInteger.valueOf(779899055L).toByteArray()),
                new Bytes(BigInteger.valueOf(779900080L).toByteArray()),
                10,
                new Bytes(BigInteger.valueOf(1000L).toByteArray()),
                5
            );
        
        Pk1Tuple actual = service.toTuplePk(tuple);
        
        assertEquals(new Pk1Tuple(
                new Pk1TupleHeaderBuilder()
                    .openSize(4)
                    .highRelative(true)
                    .highSize(1)
                    .lowRelative(true)
                    .lowSize(2)
                    .closeRelative(true)
                    .closeSize(1)
                    .decimals(10)
                    .volumeSize(2)
                    .volumeDecimals(5)
                    .build(),
                new Pk1TuplePayload(
                    new Bytes(BigInteger.valueOf(779900071L).toByteArray()),
                    new Bytes(BigInteger.valueOf(-28).toByteArray()),
                    new Bytes(BigInteger.valueOf(1016).toByteArray()),
                    new Bytes(BigInteger.valueOf(-9).toByteArray()),
                    new Bytes(BigInteger.valueOf(1000L).toByteArray())
                )
            ), actual);
    }
    
    @Test
    public void testGetHeaderSectionSize() {
        assertEquals(3, service.getHeaderSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(1)
                .highSize(2)
                .lowSize(3)
                .closeSize(4)
                .build()));
        assertEquals(7, service.getHeaderSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(7)
                .highSize(8)
                .lowSize(9)
                .closeSize(10)
                .build()));
        assertEquals(11, service.getHeaderSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(20)   // 1 byte
                .highSize(200)  // 2 bytes
                .lowSize(2000)  // 2 bytes
                .closeSize(200000) // 3 bytes
                .build()));
    }
    
    @Test
    public void testGetDecimalsSectionSize() {
        assertEquals(0, service.getDecimalsSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(3)
                .volumeDecimals(7)
                .build()));
        assertEquals(2, service.getDecimalsSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(-1)
                .volumeDecimals(25)
                .build()));
        assertEquals(0, service.getDecimalsSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(7)
                .volumeDecimals(7)
                .build()));
        assertEquals(3, service.getDecimalsSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(452)
                .volumeDecimals(3)
                .build()));
    }
    
    @Test
    public void testGetOhlcDataSectionSize() {
        assertEquals(12, service.getOhlcDataSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(5)
                .highSize(2)
                .lowSize(3)
                .closeSize(2)
                .build()));
    }
    
    @Test
    public void testGetVolumeDataSectionSize() {
        assertEquals(7, service.getVolumeDataSectionSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .volumeSize(7)
                .build()));
    }
    
    
    @Test
    public void testGetRecordSize() {
        assertEquals(25, service.getRecordSize(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(452)
                .volumeDecimals(3)
                .openSize(5)
                .highSize(2)
                .lowSize(3)
                .closeSize(2)
                .volumeSize(7)
                .build()));
        
        // header section = 3
        // decimals data section = 3
        // OHLC data section = 12
        // volume data section = 7 bytes
    }
    
    @Test
    public void testNewByteBuffer() {
        ByteBuffer actual = service.newByteBuffer(16);
        
        assertNotNull(actual);
        assertEquals(16, actual.capacity());
    }
    
    @Test
    public void testPackHeaderByte1_DecimalsInHeaderAndOhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(5)
                .volumeDecimals(3)
                .openSize(1)
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01110100;
        //   hdr_dcm_value###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackHeaderByte1_DecimalsInHeaderAndOhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(2)
                .volumeDecimals(0)
                .openSize(16) // At least one of OHLC component should be longer than 8 bytes
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b00001010;
        //   hdr_dcm_value###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderByte1_DecimalsOutsideAndOhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(256) // At least one of "decimals" should be greater than 7
                .volumeDecimals(-65590) // ...or be negative
                .openSize(1)
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01000101; // KIM: sizes are stored reduced by 1
        //   hdr_dcm_value###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderByte1_DecimalsOutsideAndOhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(99881725) // any fantastic scale
                .volumeDecimals(177855)
                .openSize(1)
                .highSize(89) // at least one of OHLC component should be longer than 8 bytes
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01001111; // KIM: sizes are stored reduced by 1
        //   hdr_dcm_value###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderOpenAndHigh_OhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(8)
                .highRelative(true)
                .highSize(3)
                .build(), dest);
        
        //              open size### ###high size
        byte expected = (byte) 0b11100101; // KIM: sizes are stored reduced by 1
        //                    unused#   #high relative
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackHeaderOpenAndHigh_OhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(148290)
                .highRelative(true)
                .highSize(2409)
                .build(), dest);
        
        //              open size### ###high size
        byte expected = (byte) 0b01000011; // KIM: sizes are stored reduced by 1
        //                    unused#   #high relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderOpenAndHigh_HighIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(1)
                .highRelative(true)
                .highSize(1)
                .build(), dest);
        
        //              open size### ###high size
        byte expected = (byte) 0b00000001; // KIM: sizes are stored reduced by 1
        //                    unused#   #high relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderOpenAndHigh_HighIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(1)
                .highRelative(false)
                .highSize(1)
                .build(), dest);
        
        //              open size### ###high size
        byte expected = (byte) 0b00000000; // KIM: sizes are stored reduced by 1
        //                    unused#   #high relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderLowAndClose_OhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(false)
                .lowSize(5)
                .closeRelative(false)
                .closeSize(2)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b10000010; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackHeaderLowAndClose_OhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(false)
                .lowSize(596112) 
                .closeRelative(false)
                .closeSize(2)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b01000000; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderLowAndClose_LowIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(false)
                .lowSize(1) 
                .closeRelative(true)
                .closeSize(8)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b00001111; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderLowAndClose_LowIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(true)
                .lowSize(1) 
                .closeRelative(true)
                .closeSize(8)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b00011111; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderLowAndClose_CloseIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(true)
                .lowSize(8) 
                .closeRelative(false)
                .closeSize(1)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b11110000; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderLowAndClose_CloseIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
                .lowRelative(true)
                .lowSize(8) 
                .closeRelative(true)
                .closeSize(1)
                .build(), dest);
        
        //               low size### ###close size
        byte expected = (byte) 0b11110001; // KIM: sizes are stored reduced by 1
        //              low relative#   #close relative
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackHeaderOhlcSizes_ShouldPackIfThereIsDedicatedSection() {
        ByteBuffer dest = ByteBuffer.allocate(10);
        
        service.packHeaderOhlcSizes(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(0x01D4C0) // 3 bytes
                .highSize(0x032C) // 2 bytes
                .lowSize(0x0F) // 1 byte
                .closeSize(0x05DA9AEA) // 4 bytes
                .build(), dest);
        
        assertArrayEquals(ByteUtils.hexStringToByteArr("01D4C0 032C 0F 05DA9AEA"), dest.array());
        assertEquals(10, dest.position());
    }
    
    @Test
    public void testPackHeaderOhlcSizes_ShouldSkipIfNotNeeded() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packHeaderOhlcSizes(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(8)
                .highSize(2)
                .lowSize(1)
                .closeSize(7)
                .build(), dest);
        
        assertEquals(0, dest.position()); // nothing has written
    }
    
    @Test
    public void testPackDecimals_ShouldPackIfThereIsDedicatedSection() {
        ByteBuffer dest = ByteBuffer.allocate(3);
        
        service.packDecimals(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(815)
                .volumeDecimals(2)
                .build(), dest);
        
        assertArrayEquals(ByteUtils.hexStringToByteArr("032F 02"), dest.array());
        assertEquals(3, dest.position());
    }
    
    @Test
    public void testPackDecimals_ShouldSkipIfNotNeeded() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packDecimals(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(5)
                .volumeDecimals(3)
                .build(), dest);
        
        assertEquals(0, dest.position());
    }
    
    @Test
    public void testPackPayload() {
        ByteBuffer dest = ByteBuffer.allocate(16);
        
        service.packPayload(new Pk1TuplePayload(
                new Bytes(BigInteger.valueOf(0x5529FE95).toByteArray()), // 4 bytes
                new Bytes(BigInteger.valueOf(0xFF).toByteArray()), // 2 bytes
                new Bytes(BigInteger.valueOf(0x092715).toByteArray()), // 3 bytes
                new Bytes(BigInteger.valueOf(0xFE2419).toByteArray()), // 4 bytes?
                new Bytes(BigInteger.valueOf(0x8027).toByteArray()) // 3 bytes
            ), dest);
        //                                              0 1 2 3  4 5  6 7 8  9 10  12 13  14
        assertArrayEquals(ByteUtils.hexStringToByteArr("5529FE95 00FF 092715 00FE2419 008027"), dest.array());
        assertEquals(16, dest.position());
    }
}
