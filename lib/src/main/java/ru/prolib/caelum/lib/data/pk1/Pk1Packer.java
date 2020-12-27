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
    
    public Bytes packTuple(ITupleData source) {
        var tuple = utils.toTuplePk(source);
        var header = tuple.header();
        var dest = utils.newByteBufferForRecord(header);
        utils.packTupleHeaderByte1(header, dest);
        utils.packTupleHeaderOpenAndHigh(header, dest);
        utils.packTupleHeaderLowAndClose(header, dest);
        utils.packTupleHeaderOhlcSizes(header, dest);
        utils.packTupleHeaderDecimals(header, dest);
        utils.packTuplePayload(tuple.payload(), dest);
        return new Bytes(dest.array());
    }
    
    public ITupleData unpackTuple(Bytes source) {
        return new Pk1TupleData(utils.unpackTupleHeader(source),
            new Bytes(source.getSource(), source.getOffset(), source.getLength()));
    }
    
}
