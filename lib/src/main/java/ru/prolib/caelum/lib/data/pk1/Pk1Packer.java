package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.RawTuple;

public class Pk1Packer {
    private final Pk1Utils utils;
    
    public Pk1Packer(Pk1Utils utils) {
        this.utils = utils;
    }
    
    public Bytes pack(RawTuple source) {
        var tuple = utils.toTuplePk(source);
        var dest = utils.newByteBuffer(utils.getRecordSize(tuple.header()));
        var header = tuple.header();
        utils.packHeaderByte1(header, dest);
        utils.packHeaderOpenAndHigh(header, dest);
        utils.packHeaderLowAndClose(header, dest);
        utils.packHeaderOhlcSizes(header, dest);
        utils.packDecimals(header, dest);
        utils.packPayload(tuple.payload(), dest);
        return new Bytes(dest.array());
    }
    
}