package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class Pk1TuplePayloadTest {
    private Bytes source, open, high, low, close, volume;
    private Pk1TuplePayload service;
    
    @Before
    public void setUp() throws Exception {
        source = new Bytes(ByteUtils.hexStringToByteArr("00FE 12AB 6F 2301 F5"));
        open = new Bytes(source.getSource(), 0, 2);
        high = new Bytes(source.getSource(), 2, 2);
        low = new Bytes(source.getSource(), 4, 1);
        close = new Bytes(source.getSource(), 5, 2);
        volume = new Bytes(source.getSource(), 7, 1);
        service = new Pk1TuplePayload(open, high, low, close, volume);
    }
    
    @Test
    public void testGetters() {
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("00FE")), service.open());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("12AB")), service.high());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("6F")), service.low());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("2301")), service.close());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("F5")), service.volume());
    }
    
    @Test
    public void testCtor_ShouldThrowIfOpenIsNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new Pk1TuplePayload(null, high, low, close, volume));
        
        assertEquals("Open bytes was not defined", e.getMessage());
    }
    
    @Test
    public void testCtor_ShouldThrowIfHighIsNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new Pk1TuplePayload(open, null, low, close, volume));
        
        assertEquals("High bytes was not defined", e.getMessage());
    }
    
    @Test
    public void testCtor_ShouldThrowIfLowIsNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new Pk1TuplePayload(open, high, null, close, volume));
        
        assertEquals("Low bytes was not defined", e.getMessage());
    }
    
    @Test
    public void testCtor_ShouldThrowIfCloseIsNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new Pk1TuplePayload(open, high, low, null, volume));
        
        assertEquals("Close bytes was not defined", e.getMessage());
    }
    
    @Test
    public void testCtor_ShouldThrowIfVolumeIsNull() {
        NullPointerException e = assertThrows(NullPointerException.class,
                () -> new Pk1TuplePayload(open, high, low, close, null));
        
        assertEquals("Volume bytes was not defined", e.getMessage());
    }

}
