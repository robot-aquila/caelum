package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.ITupleData;

public class Pk1Utils {
    
    public Pk1Tuple toTuplePk(ITupleData tuple) {
        Pk1TupleHeaderBuilder headerBuilder = new Pk1TupleHeaderBuilder();
        BigInteger o = tuple.open(), h = tuple.high(), l = tuple.low(), c = tuple.close(), v = tuple.volume();
        byte[]
            os = o.toByteArray(),
            hs = h.toByteArray(),
            ls = l.toByteArray(),
            cs = c.toByteArray(),
            vs = v.toByteArray(),
            hx = o.subtract(h).toByteArray(),
            lx = o.subtract(l).toByteArray(),
            cx = o.subtract(c).toByteArray();
        if ( hx.length < hs.length ) {
            hs = hx;
            headerBuilder.highRelative(true);
        } else {
            headerBuilder.highRelative(false);
        }
        if ( lx.length < ls.length ) {
            ls = lx;
            headerBuilder.lowRelative(true);
        } else {
            headerBuilder.lowRelative(false);
        }
        if ( cx.length < cs.length ) {
            cs = cx;
            headerBuilder.closeRelative(true);
        } else {
            headerBuilder.closeRelative(false);
        }
        return new Pk1Tuple(headerBuilder
                .openSize(os.length)
                .highSize(hs.length)
                .lowSize(ls.length)
                .closeSize(cs.length)
                .volumeSize(vs.length)
                .decimals(tuple.decimals())
                .volumeDecimals(tuple.volumeDecimals())
                .build(),
            new Pk1TuplePayload(new Bytes(os), new Bytes(hs), new Bytes(ls), new Bytes(cs), new Bytes(vs))
        );
    }
    
    public ByteBuffer newByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }
    
    public ByteBuffer newByteBufferForRecord(IPk1TupleHeader header) {
        return newByteBuffer(header.recordSize());
    }
    
    public void packTupleHeaderByte1(IPk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreNumberOfDecimalsInHeader() ) {
            dest.put((byte)(
                    ByteUtils.boolToBit(!header.canStoreOhlcSizesInHeader(), 1) |
                    ByteUtils.intToF3b(header.decimals(), 2) |
                    ByteUtils.intToF3b(header.volumeDecimals(), 5)
                ));
        } else {
            dest.put((byte)(
                    ByteUtils.boolToBit(true, 0) |
                    ByteUtils.boolToBit(!header.canStoreOhlcSizesInHeader(), 1) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.decimals()), 2) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.volumeDecimals()), 5)
                ));
        }
    }
    
    public void packTupleHeaderOpenAndHigh(IPk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() ) {
            dest.put((byte)(
                    ByteUtils.boolToBit(header.isHighRelative(), 0) |
                    ByteUtils.sizeToF3b(header.highSize(), 1) |
                    ByteUtils.sizeToF3b(header.openSize(), 5)
                ));
        } else {
            dest.put((byte)(
                    ByteUtils.boolToBit(header.isHighRelative(), 0) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.highSize()), 1) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.openSize()), 5)
                ));
        }
    }
    
    public void packTupleHeaderLowAndClose(IPk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() ) {
            dest.put((byte)(
                    ByteUtils.boolToBit(header.isCloseRelative(), 0) |
                    ByteUtils.sizeToF3b(header.closeSize(), 1) |
                    ByteUtils.boolToBit(header.isLowRelative(), 4) |
                    ByteUtils.sizeToF3b(header.lowSize(), 5)
                ));
        } else {
            dest.put((byte)(
                    ByteUtils.boolToBit(header.isCloseRelative(), 0) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.closeSize()), 1) |
                    ByteUtils.boolToBit(header.isLowRelative(), 4) |
                    ByteUtils.sizeToF3b(ByteUtils.intSize(header.lowSize()), 5)
                ));
        }
        
    }
    
    public void packTupleHeaderOhlcSizes(IPk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() == false ) {
            Bytes os, hs, ls, cs;
            dest.put((os = ByteUtils.intToBytes(header.openSize())).getSource(), os.getOffset(), os.getLength());
            dest.put((hs = ByteUtils.intToBytes(header.highSize())).getSource(), hs.getOffset(), hs.getLength());
            dest.put((ls = ByteUtils.intToBytes(header.lowSize())).getSource(), ls.getOffset(), ls.getLength());
            dest.put((cs = ByteUtils.intToBytes(header.closeSize())).getSource(), cs.getOffset(), cs.getLength());
        }
    }
    
    public void packTupleDecimals(IPk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreNumberOfDecimalsInHeader() == false ) {
            Bytes dx, vx;
            dest.put((dx = ByteUtils.intToBytes(header.decimals())).getSource(), dx.getOffset(), dx.getLength());
            dest.put((vx = ByteUtils.intToBytes(header.volumeDecimals())).getSource(), vx.getOffset(), vx.getLength());
        }
    }
    
    public void packTuplePayload(Pk1TuplePayload payload, ByteBuffer dest) {
        Bytes o, h, l, c, v;
        dest.put((o = payload.open()).getSource(), o.getOffset(), o.getLength());
        dest.put((h = payload.high()).getSource(), h.getOffset(), h.getLength());
        dest.put((l = payload.low()).getSource(), l.getOffset(), l.getLength());
        dest.put((c = payload.close()).getSource(), c.getOffset(), c.getLength());
        dest.put((v = payload.volume()).getSource(), v.getOffset(), v.getLength());
    }
    
    public IPk1TupleHeader unpackTupleHeader(Bytes bytes) {
        return new Pk1TupleHeaderWrp(bytes.getSource(), bytes.getOffset(), bytes.getLength() - bytes.getOffset());
    }
    
}
