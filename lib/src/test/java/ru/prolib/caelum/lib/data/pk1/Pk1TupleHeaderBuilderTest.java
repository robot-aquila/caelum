package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class Pk1TupleHeaderBuilderTest {
    private Pk1TupleHeaderBuilder service;
    
    @Before
    public void setUp() throws Exception {
        service = new Pk1TupleHeaderBuilder();
    }
    
    @Test
    public void testBuild() {
        Pk1TupleHeader actual = service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build();
        
        Pk1TupleHeader expected = new Pk1TupleHeader(3, 5, 12, true, 8, false, 3, true, 11, 24);
        assertEquals(expected, actual);
    }
    
    @Test
    public void testBuild_ShouldThrowIfDecimalsWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Decimals was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfVolumeDecimalsWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Volume decimals was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfOpenSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Size of opening value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfHighSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Size of highest value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfLowSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Size of lowest value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfCloseSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Size of closing value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfVolumeSizeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .highRelative(true)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Size of volume component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfHighRelativeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .lowRelative(false)
                .closeRelative(true)
                .build());
        assertEquals("Relativity of the high value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfLowRelativeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .closeRelative(true)
                .build());
        assertEquals("Relativity of the low value component was not specified", e.getMessage());
    }
    
    @Test
    public void testBuild_ShouldThrowIfCloseRelativeWasNotSpecified() {
        NullPointerException e = assertThrows(NullPointerException.class, () -> service
                .decimals(3)
                .volumeDecimals(5)
                .openSize(12)
                .highSize(8)
                .lowSize(3)
                .closeSize(11)
                .volumeSize(24)
                .highRelative(true)
                .lowRelative(false)
                .build());
        assertEquals("Relativity of the close value component was not specified", e.getMessage());
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
        service.decimals(3).volumeDecimals(5)
            .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
            .highRelative(true).lowRelative(false).closeRelative(true);
        
        assertTrue(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(4).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(6)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(13).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(9).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(4).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(12).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(25)
                .highRelative(true).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(false).lowRelative(false).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(true).closeRelative(true)));
        assertFalse(service.equals(new Pk1TupleHeaderBuilder().decimals(3).volumeDecimals(5)
                .openSize(12).highSize(8).lowSize(3).closeSize(11).volumeSize(24)
                .highRelative(true).lowRelative(false).closeRelative(false)));
    }
    
    @Test
    public void testToString() {
        service.decimals(3)
            .volumeDecimals(5)
            .openSize(12)
            .highSize(8)
            .lowSize(3)
            .closeSize(11)
            .volumeSize(24)
            .highRelative(true)
            .lowRelative(false)
            .closeRelative(true);
        
        String expected = new StringBuilder()
                .append("Pk1TupleHeaderBuilder[decimals=3,volumeDecimals=5,")
                .append("openSize=12,highRelative=true,highSize=8,lowRelative=false,lowSize=3,")
                .append("closeRelative=true,closeSize=11,volumeSize=24]")
                .toString();
        assertEquals(expected, service.toString());
    }

}
