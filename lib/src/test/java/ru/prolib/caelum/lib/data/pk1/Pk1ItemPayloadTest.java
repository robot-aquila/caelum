package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class Pk1ItemPayloadTest {
    private Bytes source, value, volume, customData;
    private Pk1ItemPayload service;
    
    @Before
    public void setUp() throws Exception {
        source = new Bytes(ByteUtils.hexStringToByteArr("FEFE251796 A000 6F2301F5"));
        value = new Bytes(source.getSource(), 0, 5);
        volume = new Bytes(source.getSource(), 5, 2);
        customData = new Bytes(source.getSource(), 7, 4);
        service = new Pk1ItemPayload(value, volume, customData);
    }
    
    @Test
    public void testGetters() {
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("FEFE251796")), service.value());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("A000")), service.volume());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("6F2301F5")), service.customData());
    }
    
    @Test
    public void testCtor_ShouldAcceptNullValue() {
        service = new Pk1ItemPayload(null, volume, customData);
        assertNull(service.value());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("A000")), service.volume());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("6F2301F5")), service.customData());
    }
    
    @Test
    public void testCtor_ShouldAcceptNullVolume() {
        service = new Pk1ItemPayload(value, null, customData);
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("FEFE251796")), service.value());
        assertNull(service.volume());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("6F2301F5")), service.customData());
    }
    
    @Test
    public void testCtor_ShouldAcceptNullCustomData() {
        service = new Pk1ItemPayload(value, volume, null);
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("FEFE251796")), service.value());
        assertEquals(new Bytes(ByteUtils.hexStringToByteArr("A000")), service.volume());
        assertNull(service.customData());
    }

}
