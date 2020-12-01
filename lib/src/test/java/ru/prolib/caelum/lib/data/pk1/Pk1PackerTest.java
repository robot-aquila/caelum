package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.lib.data.pk1.Pk1TestUtils.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;
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
    public void testPack_RawTuple() {
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

}
