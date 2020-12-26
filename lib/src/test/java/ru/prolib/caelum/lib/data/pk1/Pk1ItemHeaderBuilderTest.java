package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class Pk1ItemHeaderBuilderTest {
    private Pk1ItemHeaderBuilder service;
    
    @Before
    public void setUp() throws Exception {
        service = new Pk1ItemHeaderBuilder();
    }
    
    @Test
    public void testBuild() {
        Pk1ItemHeader actual = service
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)
                .build();
        
        Pk1ItemHeader expected = new Pk1ItemHeader(5, 3, 6, 2, 4);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBuild_ShouldThrowIfDecimalsWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)
                .build());
        assertEquals("Decimals was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfVolumeDecimalsWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(5)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)
                .build());
        assertEquals("Volume decimals was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfValueSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(5)
                .volumeDecimals(3)
                .volumeSize(2)
                .customDataSize(4)
                .build());
        assertEquals("Value size was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfVolumeSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .customDataSize(4)
                .build());
        assertEquals("Volume size was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfCustomDataSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .build());
        assertEquals("Custom data size was not specified", e.getMessage());
    }
    
    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals_SpecialCases() {
        assertTrue(service.equals(service));
        assertFalse(service.equals(this));
        assertFalse(service.equals(null));
    }
    
    @Test
    public void testEquals() {
        service.decimals(5)
            .volumeDecimals(3)
            .valueSize(6)
            .volumeSize(2)
            .customDataSize(4);
        
        assertTrue(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(6)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(5)
                .volumeDecimals(4)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(4)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(7)
                .volumeSize(2)
                .customDataSize(4)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(3)
                .customDataSize(4)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(5)
                .volumeDecimals(3)
                .valueSize(6)
                .volumeSize(2)
                .customDataSize(5)));
        assertFalse(service.equals(new Pk1ItemHeaderBuilder()
                .decimals(6)
                .volumeDecimals(4)
                .valueSize(7)
                .volumeDecimals(3)
                .customDataSize(5)));
    }
    
    @Test
    public void testToString() {
        service.decimals(5)
            .volumeDecimals(3)
            .valueSize(6)
            .volumeSize(2)
            .customDataSize(4);
        
        String expected = new StringBuilder()
                .append("Pk1ItemHeaderBuilder[decimals=5,volumeDecimals=3,valueSize=6,volumeSize=2,customDataSize=4]")
                .toString();
        assertEquals(expected, service.toString());
    }

}
