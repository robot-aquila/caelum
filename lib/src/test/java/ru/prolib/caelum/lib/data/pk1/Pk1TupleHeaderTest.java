package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

public class Pk1TupleHeaderTest {
    private Pk1TupleHeader service;

    @Before
    public void setUp() throws Exception {
        service = new Pk1TupleHeader(5, 3, 8, true, 7, false, 3, true, 2, 4);
    }
    
    @Test
    public void testGetters() {
        assertEquals(5, service.decimals());
        assertEquals(3, service.volumeDecimals());
        assertEquals(8, service.openSize());
        assertTrue(service.isHighRelative());
        assertEquals(7, service.highSize());
        assertFalse(service.isLowRelative());
        assertEquals(3, service.lowSize());
        assertTrue(service.isCloseRelative());
        assertEquals(2, service.closeSize());
        assertEquals(4, service.volumeSize());
    }
    
    @Test
    public void testIsA_IPk1TupleHeader() {
        assertThat(service, instanceOf(IPk1TupleHeader.class));
    }
    
    @Test
    public void testToString() {
        String expected = new StringBuilder()
                .append("Pk1TupleHeader[decimals=5, volumeDecimals=3, openSize=8, ")
                .append("isHighRelative=true, highSize=7, isLowRelative=false, lowSize=3, ")
                .append("isCloseRelative=true, closeSize=2, volumeSize=4]")
                .toString();
        
        assertEquals(expected, service.toString());
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
        assertTrue(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(6, 3, 8, true, 7, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 4, 8, true, 7, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 9, true, 7, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, false, 7, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 8, false, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, true, 3, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, false, 4, true, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, false, 3, false, 2, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, false, 3, true, 3, 4)));
        assertFalse(service.equals(new Pk1TupleHeader(5, 3, 8, true, 7, false, 3, true, 2, 5)));
    }
    
    @Test
    public void testCanStoreNumberOfDecimalsInHeader() {
        Object[][] fixture = {
                {  0,  0, true  },
                { -1,  0, false },
                {  0, -1, false },
                {  1,  0, true  },
                {  0,  1, true  },
                {  1,  1, true  },
                { -1,  7, false },
                {  7, -1, false },
                {  7,  7, true  },
                {  8,  0, false },
                {  0,  8, false },
                {  8,  8, false },
        };
        
        for ( int i = 0; i < fixture.length; i ++  ) {
            int decimals = (int) fixture[i][0], volumeDecimals = (int) fixture[i][1];
            var expected = (boolean) fixture[i][2];
            var header = new Pk1TupleHeader(decimals, volumeDecimals, 1, true, 1, true, 1, true, 1, 1);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for decimals=").append(decimals)
                    .append(" volumeDecimals=").append(volumeDecimals)
                    .toString(), expected, header.canStoreNumberOfDecimalsInHeader());
        }
    }
    
    @Test
    public void testCanStoreOhlcSizesInHeader() {
        Object[][] fixture = {
                // open size, high size, low size, close size, expected result
                {  1,  1,  1,  1, true  },
                {  0,  0,  0,  0, false }, // impossible case but should answer
                {  8,  8,  8,  8, true  },
                { 10,  8,  8,  8, false },
                {  8, 10,  8,  8, false },
                {  8,  8, 10,  8, false },
                {  8,  8,  8, 10, false },
                {  0,  5,  5,  5, false },
                {  5, -1,  4,  2, false }, // impossible but should answer
                {  1,  2,  3,  4, true  }
        };
        
        for ( int i = 0; i < fixture.length; i ++ ) {
            int openSize = (int) fixture[i][0], highSize = (int) fixture[i][1],
                lowSize = (int) fixture[i][2], closeSize = (int) fixture[i][3];
            var expected = (boolean) fixture[i][4];
            var header = new Pk1TupleHeader(0, 0, openSize, true, highSize, true, lowSize, true, closeSize, 1);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for openSize=").append(openSize).append(" highSize=").append(highSize)
                    .append(" lowSize=").append(lowSize).append(" closeSize=").append(closeSize)
                    .toString(), expected, header.canStoreOhlcSizesInHeader());
        }
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        assertEquals(3, new Pk1TupleHeader(5, 3, 1, false, 1, false, 1, false, 1, 1).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanStoreOhlcSizesInHeader() {
        assertEquals(5, new Pk1TupleHeader(10, 3, 1, false, 1, false, 1, false, 1, 1).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        assertEquals(9, new Pk1TupleHeader(5, 3, 1, false, 1, false, 0x00EB12, false, 10, 1).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreOhlcSizesInHeader() {
        assertEquals(10, new Pk1TupleHeader(5, 10, 1, false, 0x1224, false, 1, false, 1, 1).headerSize());
    }
    
    @Test
    public void testRecordSize() {
        assertEquals(8, new Pk1TupleHeader(5, 3, 1, false, 1, false, 1, false, 1, 1).recordSize());
        assertEquals(10, new Pk1TupleHeader(5, 12, 1, false, 1, false, 1, false, 1, 1).recordSize());
        // os=3, hs=2, ls=1, cs=4
        // header size=3 + 3 + 2 + 1 + 4 = 13
        // total: 13 + 182231 + 1511 + 26 + 19009122 + 32 =         
        assertEquals(19192935, new Pk1TupleHeader(5, 3, 182231, false, 1511, false, 26, false, 19009122, 32).recordSize());
        assertEquals(19192937, new Pk1TupleHeader(25, 3, 182231, false, 1511, false, 26, false, 19009122, 32).recordSize());
    }

}
