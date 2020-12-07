package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.ITupleData;

public class Pk1Packer {
    private final Pk1Utils utils;
    
    public Pk1Packer(Pk1Utils utils) {
        this.utils = utils;
    }
    
    public Pk1Packer() {
        this(new Pk1Utils());
    }
    
    public Bytes pack(ITupleData source) {
        var tuple = utils.toTuplePk(source);
        var header = tuple.header();
        var dest = utils.newByteBufferForRecord(header);
        utils.packHeaderByte1(header, dest);
        utils.packHeaderOpenAndHigh(header, dest);
        utils.packHeaderLowAndClose(header, dest);
        utils.packHeaderOhlcSizes(header, dest);
        utils.packDecimals(header, dest);
        utils.packPayload(tuple.payload(), dest);
        return new Bytes(dest.array());
    }
    
    public ITupleData unpack(Bytes source) {
        return new Pk1TupleData(utils.unpackHeader(source),
            new Bytes(source.getSource(), source.getOffset(), source.getLength()));
    }
    
}
