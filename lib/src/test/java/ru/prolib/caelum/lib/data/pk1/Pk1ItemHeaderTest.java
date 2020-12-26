package ru.prolib.caelum.lib.data.pk1;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class Pk1ItemHeaderTest {
    private Pk1ItemHeader service;
    
    @Before
    public void setUp() throws Exception {
        service = new Pk1ItemHeader(3, 5, 1, 2, 10);
    }
    
    @Test
    public void testGetters() {
        assertEquals(3, service.decimals());
        assertEquals(5, service.volumeDecimals());
        assertEquals(1, service.valueSize());
        assertEquals(2, service.volumeSize());
        assertEquals(10, service.customDataSize());
    }
    
    @Test
    public void testIsA_IPk1ItemHeader() {
        assertThat(service, instanceOf(IPk1ItemHeader.class));
    }
    
    @Test
    public void testToString() {
        String expected = new StringBuilder()
                .append("Pk1ItemHeader[decimals=3, volumeDecimals=5, valueSize=1, volumeSize=2, customDataSize=10]")
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
        assertTrue(service.equals(new Pk1ItemHeader(3, 5, 1, 2, 10)));
        assertFalse(service.equals(new Pk1ItemHeader(4, 5, 1, 2, 10)));
        assertFalse(service.equals(new Pk1ItemHeader(3, 6, 1, 2, 10)));
        assertFalse(service.equals(new Pk1ItemHeader(3, 5, 2, 2, 10)));
        assertFalse(service.equals(new Pk1ItemHeader(3, 5, 1, 3, 10)));
        assertFalse(service.equals(new Pk1ItemHeader(3, 5, 1, 2, 11)));
        assertFalse(service.equals(new Pk1ItemHeader(4, 6, 2, 3, 11)));
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
            var header = new Pk1ItemHeader(decimals, volumeDecimals, 1, 1, 1);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for decimals=").append(decimals)
                    .append(" volumeDecimals=").append(volumeDecimals)
                    .toString(), expected, header.canStoreNumberOfDecimalsInHeader());
        }
    }
    
    @Test
    public void testIsValuePresent() {
        Object[][] fixture = {
                // value size, expected result
                { -9, false },
                { -1, false },
                {  0, false },
                {  1, true  },
                { 15, true  },
                { 99, true  },
        };
        
        for ( int i = 0; i < fixture.length; i ++ ) {
            int valueSize = (int) fixture[i][0];
            var expected = (boolean) fixture[i][1];
            var header = new Pk1ItemHeader(0, 0, valueSize, 0, 0);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for valueSize=").append(valueSize)
                    .toString(), expected, header.isValuePresent());
        }
    }
    
    @Test
    public void testIsVolumePresent() {
        Object[][] fixture = {
                // volume size, expected result
                { -9, false },
                { -1, false },
                {  0, false },
                {  1, true  },
                { 15, true  },
                { 99, true  },
        };
        
        for ( int i = 0; i < fixture.length; i ++ ) {
            int volumeSize = (int) fixture[i][0];
            var expected = (boolean) fixture[i][1];
            var header = new Pk1ItemHeader(0, 0, 0, volumeSize, 0);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for volumeSize=").append(volumeSize)
                    .toString(), expected, header.isVolumePresent());
        }
    }
    
    @Test
    public void testCanStoreSizesInHeader() {
        Object[][] fixture = {
                // value size, volume size, expected result
                {  1,  1, true  },
                {  8,  8, true  },
                {  0,  0, true  }, // zeroes are allowed
                {  1,  0, true  },
                {  0,  1, true  },
                { 10,  8, false },
                {  8, 10, false },
                {  5, -1, false }, // impossible but should answer
                { -1,  5, false }, // impossible but should answer
        };
        
        for ( int i = 0; i < fixture.length; i ++ ) {
            int valueSize = (int) fixture[i][0], volumeSize = (int) fixture[i][1];
            var expected = (boolean) fixture[i][2];
            var header = new Pk1ItemHeader(0, 0, valueSize, volumeSize, 0);
            assertEquals(new StringBuilder()
                    .append("Unexpected result for valueSize=").append(valueSize)
                    .append(" volumeSize=").append(volumeSize)
                    .toString(), expected, header.canStoreSizesInHeader());
        }
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanStoreSizesInHeader() {
        assertEquals(2, new Pk1ItemHeader(3, 5, 0, 2, 0).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanStoreSizesInHeader() {
        assertEquals(4, new Pk1ItemHeader(16, 0, 1, 2, 3).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreSizesInHeader() {
        assertEquals(5, new Pk1ItemHeader(3, 5, 12, 412, 10).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanNotStoreDecimalsInHeader_CanNotStoreSizesInHeader() {
        assertEquals(8, new Pk1ItemHeader(12, 0, 17238, 7721, 0).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreSizesInHeader_ValueIsNotDefined() {
        assertEquals(3, new Pk1ItemHeader(5, 0, 0, 113, 0).headerSize());
    }
    
    @Test
    public void testHeaderSize_CanStoreDecimalsInHeader_CanNotStoreSizesInHeader_VolumeIsNotDefined() {
        assertEquals(4, new Pk1ItemHeader(0, 0, 712, 0, 0).headerSize());
    }
    
    @Test
    public void testRecordSize() {
        assertEquals(    4, new Pk1ItemHeader(3, 5, 0, 2, 0).recordSize()); 
        assertEquals(   10, new Pk1ItemHeader(16, 0, 1, 2, 3).recordSize());
        assertEquals(  439, new Pk1ItemHeader(3, 5, 12, 412, 10).recordSize());
        assertEquals(24967, new Pk1ItemHeader(12, 0, 17238, 7721, 0).recordSize());
        assertEquals(  116, new Pk1ItemHeader(5, 0, 0, 113, 0).recordSize());
        assertEquals(  716, new Pk1ItemHeader(0, 0, 712, 0, 0).recordSize());
    }

}
