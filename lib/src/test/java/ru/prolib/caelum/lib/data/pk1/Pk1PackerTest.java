package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.lib.data.pk1.Pk1TestUtils.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.ItemData;
import ru.prolib.caelum.lib.data.TupleData;

public class Pk1PackerTest {
    private IMocksControl control;
    private Pk1Utils utilsMock;
    private IPk1TupleHeader tupleHeaderMock;
    private IPk1ItemHeader itemHeaderMock;
    private Pk1Packer service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        utilsMock = control.createMock(Pk1Utils.class);
        tupleHeaderMock = control.createMock(IPk1TupleHeader.class);
        itemHeaderMock = control.createMock(IPk1ItemHeader.class);
        service = new Pk1Packer(utilsMock);
    }

    @Test
    public void testPackTuple() {
        var doesNotMatter = BigInteger.ONE;
        var source = new TupleData(doesNotMatter, doesNotMatter, doesNotMatter, doesNotMatter, 0, doesNotMatter, 0);
        var dest = ByteBuffer.allocate(24);
        var payload = tuplePayloadRandom();
        expect(utilsMock.toPk1Tuple(source)).andReturn(new Pk1Tuple(tupleHeaderMock, payload));
        expect(utilsMock.newByteBufferForRecord(tupleHeaderMock)).andReturn(dest);
        utilsMock.packTupleHeaderByte1(tupleHeaderMock, dest);
        utilsMock.packTupleHeaderOpenAndHigh(tupleHeaderMock, dest);
        utilsMock.packTupleHeaderLowAndClose(tupleHeaderMock, dest);
        utilsMock.packTupleHeaderOhlcSizes(tupleHeaderMock, dest);
        utilsMock.packTupleHeaderDecimals(tupleHeaderMock, dest);
        utilsMock.packTuplePayload(payload, dest);
        control.replay();
        
        var actual = service.packTuple(source);
        
        control.verify();
        assertSame(dest.array(), actual.getSource());
    }
    
    @Test
    public void testUnpackTuple() {
        byte[] bytes;
        Bytes source = new Bytes(bytes = new byte[100], 25, 40);
        expect(utilsMock.unpackTupleHeader(source)).andReturn(tupleHeaderMock);
        control.replay();
        
        var actual = service.unpackTuple(source);
        
        control.verify();
        var expected = new Pk1TupleData(tupleHeaderMock, new Bytes(bytes, 25, 40));
        assertEquals(expected, actual);
    }
    
    @Test
    public void testPackTuple_FullCycle_SmallValues() {
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
        
        var actual = service.unpackTuple(service.packTuple(source));
        
        assertEquals(source, actual);
    }
    
    @Test
    public void testPackTuple_FullCycle_BigValues() {
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
        
        var actual = service.unpackTuple(service.packTuple(source));
        
        assertEquals(source, actual);
    }
    
    @Test
    public void testPackItem() {
        var doesNotMatter = BigInteger.ONE;
        var source = new ItemData(doesNotMatter, 0, doesNotMatter, 0, null);
        var dest = ByteBuffer.allocate(12);
        var payload = itemPayloadRandom();
        expect(utilsMock.toPk1Item(source)).andReturn(new Pk1Item(itemHeaderMock, payload));
        expect(utilsMock.newByteBufferForRecord(itemHeaderMock)).andReturn(dest);
        utilsMock.packItemHeaderByte1(itemHeaderMock, dest);
        utilsMock.packItemHeaderValVol(itemHeaderMock, dest);
        utilsMock.packItemHeaderSizes(itemHeaderMock, dest);
        utilsMock.packItemHeaderDecimals(itemHeaderMock, dest);
        utilsMock.packItemPayload(payload, dest);
        control.replay();
        
        var actual = service.packItem(source);
        
        control.verify();
        assertSame(dest.array(), actual.getSource());
    }
    
    @Ignore
    @Test
    public void testUnpackItem() {
        fail();
    }
    
    @Ignore
    @Test
    public void testPackItem_FullCycle_SmallValues() {
        fail();
    }
    
    @Ignore
    @Test
    public void testPackItem_FullCycle_BigValues() {
        fail();
    }
}
