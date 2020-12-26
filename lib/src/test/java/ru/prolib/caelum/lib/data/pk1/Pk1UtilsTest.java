package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.TupleData;

public class Pk1UtilsTest {
    private IMocksControl control;
    private Pk1Utils service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        service = new Pk1Utils();
    }

    @Test
    public void testToTuplePk_AllOhlcComponentsAbsolute() {
        TupleData tuple = new TupleData(
                BigInteger.valueOf(17289L),
                BigInteger.valueOf(5000009912435L),
                BigInteger.valueOf(-98),
                BigInteger.valueOf(-23),
                4,
                BigInteger.valueOf(100000L),
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
        TupleData tuple = new TupleData(
                BigInteger.valueOf(779900071L),
                BigInteger.valueOf(779900099L),
                BigInteger.valueOf(779899055L),
                BigInteger.valueOf(779900080L),
                10,
                BigInteger.valueOf(1000L),
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
    public void testNewByteBuffer() {
        ByteBuffer actual = service.newByteBuffer(16);
        
        assertNotNull(actual);
        assertEquals(16, actual.capacity());
    }
    
    @Test
    public void testNewByteBufferForRecord() {
        IPk1Header headerMock = control.createMock(IPk1Header.class);
        expect(headerMock.recordSize()).andReturn(34);
        control.replay();
        
        ByteBuffer actual = service.newByteBufferForRecord(headerMock);
        
        control.verify();
        assertNotNull(actual);
        assertEquals(34, actual.capacity());
    }
    
    @Test
    public void testPackTupleHeaderByte1_DecimalsInHeaderAndOhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(5)
                .volumeDecimals(3)
                .openSize(1)
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01110100;
        //  hdr_dcm_volume###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackTupleHeaderByte1_DecimalsInHeaderAndOhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(2)
                .volumeDecimals(0)
                .openSize(16) // At least one of OHLC component should be longer than 8 bytes
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b00001010;
        //  hdr_dcm_volume###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackTupleHeaderByte1_DecimalsOutsideAndOhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(256) // At least one of "decimals" should be greater than 7
                .volumeDecimals(-65590) // ...or be negative
                .openSize(1)
                .highSize(1)
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01000101; // KIM: sizes are stored reduced by 1
        //  hdr_dcm_volume###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackTupleHeaderByte1_DecimalsOutsideAndOhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderByte1(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(99881725) // any fantastic scale
                .volumeDecimals(177855)
                .openSize(1)
                .highSize(89) // at least one of OHLC component should be longer than 8 bytes
                .lowSize(1)
                .closeSize(1)
                .build(), dest);
        
        //       hdr_dcm_ohlc### #hdr_mp_dcm
        byte expected = 0b01001111; // KIM: sizes are stored reduced by 1
        //  hdr_dcm_volume###   #hdr_mp_ohlc     
        assertEquals(expected, dest.get(0));
    }
    
    @Test
    public void testPackTupleHeaderOpenAndHigh_OhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderOpenAndHigh_OhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderOpenAndHigh_HighIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderOpenAndHigh_HighIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderOpenAndHigh(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_OhlcSizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_OhlcSizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_LowIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_LowIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_CloseIsAbsolute() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderLowAndClose_CloseIsRelative() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderLowAndClose(Pk1TestUtils.tupleHeaderBuilderRandom()
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
    public void testPackTupleHeaderOhlcSizes_ShouldPackIfThereIsDedicatedSection() {
        ByteBuffer dest = ByteBuffer.allocate(10);
        
        service.packTupleHeaderOhlcSizes(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(0x01D4C0) // 3 bytes
                .highSize(0x032C) // 2 bytes
                .lowSize(0x0F) // 1 byte
                .closeSize(0x05DA9AEA) // 4 bytes
                .build(), dest);
        
        assertArrayEquals(ByteUtils.hexStringToByteArr("01D4C0 032C 0F 05DA9AEA"), dest.array());
        assertEquals(10, dest.position());
    }
    
    @Test
    public void testPackTupleHeaderOhlcSizes_ShouldSkipIfNotNeeded() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleHeaderOhlcSizes(Pk1TestUtils.tupleHeaderBuilderRandom()
                .openSize(8)
                .highSize(2)
                .lowSize(1)
                .closeSize(7)
                .build(), dest);
        
        assertEquals(0, dest.position()); // nothing has written
    }
    
    @Test
    public void testPackTupleDecimals_ShouldPackIfThereIsDedicatedSection() {
        ByteBuffer dest = ByteBuffer.allocate(3);
        
        service.packTupleDecimals(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(815)
                .volumeDecimals(2)
                .build(), dest);
        
        assertArrayEquals(ByteUtils.hexStringToByteArr("032F 02"), dest.array());
        assertEquals(3, dest.position());
    }
    
    @Test
    public void testPackTupleDecimals_ShouldSkipIfNotNeeded() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packTupleDecimals(Pk1TestUtils.tupleHeaderBuilderRandom()
                .decimals(5)
                .volumeDecimals(3)
                .build(), dest);
        
        assertEquals(0, dest.position());
    }
    
    @Test
    public void testPackTuplePayload() {
        ByteBuffer dest = ByteBuffer.allocate(16);
        
        service.packTuplePayload(new Pk1TuplePayload(
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
    
    @Test
    public void testUnpackTupleHeader() {
        Bytes source = new Bytes(new byte[100], 5, 95);
        
        Pk1TupleHeaderWrp actual = (Pk1TupleHeaderWrp) service.unpackTupleHeader(source);
        
        assertNotNull(actual);
        assertSame(source.getSource(),actual.getBytes());
        assertEquals(5, source.getOffset());
        assertEquals(95, source.getLength());
    }
    
    @Test
    public void testPackItemHeaderByte1_DecimalsInHeader_SizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packItemHeaderByte1(Pk1TestUtils.itemHeaderBuilderRandom()
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(1)
                .volumeSize(1)
                .customDataSize(0)
                .build(), dest);
        
        //      hdr_dcm_value### #hdr_mp_dcm
        byte expected = 0b01110100;
        //  hdr_dcm_volume###   #hdr_mp_valvol
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackItemHeaderByte1_DecimalsInHeader_SizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packItemHeaderByte1(Pk1TestUtils.itemHeaderBuilderRandom()
                .decimals(2)
                .volumeDecimals(7)
                .valueSize(12)
                .volumeSize(446)
                .customDataSize(100)
                .build(), dest);
        
        //            hdr_dcm_value### #hdr_mp_dcm
        byte expected = (byte)0b11101010;
        //        hdr_dcm_volume###   #hdr_mp_valvol
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackItemHeaderByte1_DecimalsOutside_SizesInHeader() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packItemHeaderByte1(Pk1TestUtils.itemHeaderBuilderRandom()
                .decimals(274) // 2 bytes
                .volumeDecimals(574100) // 3 bytes
                .valueSize(2)
                .volumeSize(5)
                .customDataSize(100)
                .build(), dest);
        
        //            hdr_dcm_value### #hdr_mp_dcm
        byte expected = (byte)0b01000101;
        //        hdr_dcm_volume###   #hdr_mp_valvol
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
    @Test
    public void testPackItemHeaderByte1_DecimalsOutside_SizesOutside() {
        ByteBuffer dest = ByteBuffer.allocate(1);
        
        service.packItemHeaderByte1(Pk1TestUtils.itemHeaderBuilderRandom()
                .decimals(40021) // 3 bytes
                .volumeDecimals(26091) // 2 bytes
                .valueSize(900721) // 3 bytes
                .volumeSize(0) // 0 bytes
                .customDataSize(100)
                .build(), dest);
        
        //            hdr_dcm_value### #hdr_mp_dcm
        byte expected = (byte)0b00101011;
        //        hdr_dcm_volume###   #hdr_mp_valvol
        assertEquals(expected, dest.get(0));
        assertEquals(1, dest.position());
    }
    
}
