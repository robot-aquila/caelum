package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.ITupleData;

public class Pk1TupleData implements ITupleData {
    private final IPk1TupleHeader header;
    private final Bytes bytes;
    
    public Pk1TupleData(IPk1TupleHeader header, Bytes bytes) {
        this.header = header;
        this.bytes = bytes;
    }
    
    @Override
    public BigInteger open() {
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize(),
                header.openSize()
            );
    }

    @Override
    public BigInteger high() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize(),
                header.highSize()
            );
        return header.isHighRelative() ? open().add(x) : x;
    }

    @Override
    public BigInteger low() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize() + header.highSize(),
                header.lowSize()
            );
        return header.isLowRelative() ? open().add(x) : x;
    }

    @Override
    public BigInteger close() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize() + header.highSize() + header.lowSize(),
                header.closeSize()
            );
        return header.isCloseRelative() ? open().add(x) : x;
    }

    @Override
    public int decimals() {
        return header.decimals();
    }

    @Override
    public BigInteger volume() {
        int volumeSize = header.volumeSize();
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.recordSize() - volumeSize,
                volumeSize
            );
    }

    @Override
    public int volumeDecimals() {
        return header.volumeDecimals();
    }

}
