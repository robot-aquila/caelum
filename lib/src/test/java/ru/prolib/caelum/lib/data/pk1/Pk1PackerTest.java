package ru.prolib.caelum.lib.data.pk1;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.lib.data.pk1.Pk1TestUtils.*;

import java.nio.ByteBuffer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;
import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.RawTuple;

public class Pk1PackerTest {
    private IMocksControl control;
    private Pk1Utils utilsMock;
    private Pk1Packer service;

    @Before
    public void setUp() throws Exception {
        control = createStrictControl();
        utilsMock = control.createMock(Pk1Utils.class);
        service = new Pk1Packer(utilsMock);
    }

    @Test
    public void testPack_RawTuple() {
        var doesNotMatter = new Bytes(1);
        var source = new RawTuple(doesNotMatter, doesNotMatter, doesNotMatter, doesNotMatter, 0, doesNotMatter, 0);
        var dest = ByteBuffer.allocate(24);
        var header = tupleHeaderBuilderRandom().build();
        var payload = tuplePayloadRandom();
        expect(utilsMock.toTuplePk(source)).andReturn(new Pk1Tuple(header, payload));
        expect(utilsMock.getRecordSize(header)).andReturn(15);
        expect(utilsMock.newByteBuffer(15)).andReturn(dest);
        utilsMock.packHeaderByte1(header, dest);
        utilsMock.packHeaderOpenAndHigh(header, dest);
        utilsMock.packHeaderLowAndClose(header, dest);
        utilsMock.packHeaderOhlcSizes(header, dest);
        utilsMock.packDecimals(header, dest);
        utilsMock.packPayload(payload, dest);
        control.replay();
        
        var actual = service.pack(source);
        
        control.verify();
        assertSame(dest.array(), actual.getSource());
    }

}
