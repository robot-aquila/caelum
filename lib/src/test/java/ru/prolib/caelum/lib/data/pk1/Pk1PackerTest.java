package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.lib.data.pk1.Pk1TestUtils.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.TupleData;

public class Pk1PackerTest {
    private IMocksControl control;
    private Pk1Utils utilsMock;
    private IPk1TupleHeader headerMock;
    private Pk1Packer service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        utilsMock = control.createMock(Pk1Utils.class);
        headerMock = control.createMock(IPk1TupleHeader.class);
        service = new Pk1Packer(utilsMock);
    }

    @Test
    public void testPack() {
        var doesNotMatter = BigInteger.ONE;
        var source = new TupleData(doesNotMatter, doesNotMatter, doesNotMatter, doesNotMatter, 0, doesNotMatter, 0);
        var dest = ByteBuffer.allocate(24);
        var payload = tuplePayloadRandom();
        expect(utilsMock.toTuplePk(source)).andReturn(new Pk1Tuple(headerMock, payload));
        expect(utilsMock.newByteBufferForRecord(headerMock)).andReturn(dest);
        utilsMock.packHeaderByte1(headerMock, dest);
        utilsMock.packHeaderOpenAndHigh(headerMock, dest);
        utilsMock.packHeaderLowAndClose(headerMock, dest);
        utilsMock.packHeaderOhlcSizes(headerMock, dest);
        utilsMock.packDecimals(headerMock, dest);
        utilsMock.packPayload(payload, dest);
        control.replay();
        
        var actual = service.pack(source);
        
        control.verify();
        assertSame(dest.array(), actual.getSource());
    }
    
    @Test
    public void testUnpack() {
        byte[] bytes;
        Bytes source = new Bytes(bytes = new byte[100], 25, 40);
        expect(utilsMock.unpackHeader(source)).andReturn(headerMock);
        control.replay();
        
        var actual = service.unpack(source);
        
        control.verify();
        var expected = new Pk1TupleData(headerMock, new Bytes(bytes, 25, 40));
        assertEquals(expected, actual);
    }
    
    @Test
    public void testFullCycle_SmallValues() {
        service = new Pk1Packer();
        var source = new TupleData(
                BigInteger.valueOf(670294L),
                BigInteger.valueOf(3L),
                BigInteger.valueOf(670300L), // should cause relative value to pack
                BigInteger.valueOf(670200L), // should cause relative value to pack
                5,
                BigInteger.valueOf(1000L),
                3
            );
        
        var actual = service.unpack(service.pack(source));
        
        assertEquals(source, actual);
    }
    
    @Test
    public void testFullCycle_BigValues() {
        service = new Pk1Packer();
        var source = new TupleData(
                BigInteger.valueOf(81230294L).pow(26),
                BigInteger.valueOf(62828299L).pow(10),
                BigInteger.valueOf(11157200L).pow(7),
                BigInteger.valueOf(75925482L).pow(3),
                6928266,
                BigInteger.valueOf(1000L).pow(13),
                88919722
            );
        
        var actual = service.unpack(service.pack(source));
        
        assertEquals(source, actual);
    }
    
}
