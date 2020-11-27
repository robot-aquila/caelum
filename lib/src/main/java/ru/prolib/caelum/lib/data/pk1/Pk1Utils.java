package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.RawTuple;

public class Pk1Utils {
    private final ByteUtils byteUtils;
    
    public Pk1Utils(ByteUtils byteUtils) {
        this.byteUtils = byteUtils;
    }
    
    public Pk1Tuple toTuplePk(RawTuple tuple) {
        Pk1TupleHeaderBuilder headerBuilder = new Pk1TupleHeaderBuilder();
        Bytes os, hs, ls, cs, vs = tuple.volume();
        BigInteger
            o = new BigInteger((os = tuple.open()).copyBytes()),
            h = new BigInteger((hs = tuple.high()).copyBytes()),
            l = new BigInteger((ls = tuple.low()).copyBytes()),
            c = new BigInteger((cs = tuple.close()).copyBytes());
        Bytes
            hx = new Bytes(o.subtract(h).toByteArray()),
            lx = new Bytes(o.subtract(l).toByteArray()),
            cx = new Bytes(o.subtract(c).toByteArray());
        if ( hx.getLength() < hs.getLength() ) {
            hs = hx;
            headerBuilder.highRelative(true);
        } else {
            headerBuilder.highRelative(false);
        }
        if ( lx.getLength() < ls.getLength() ) {
            ls = lx;
            headerBuilder.lowRelative(true);
        } else {
            headerBuilder.lowRelative(false);
        }
        if ( cx.getLength() < cs.getLength() ) {
            cs = cx;
            headerBuilder.closeRelative(true);
        } else {
            headerBuilder.closeRelative(false);
        }
        return new Pk1Tuple(headerBuilder
                .openSize(os.getLength())
                .highSize(hs.getLength())
                .lowSize(ls.getLength())
                .closeSize(cs.getLength())
                .volumeSize(vs.getLength())
                .decimals(tuple.decimals())
                .volumeDecimals(tuple.volumeDecimals())
                .build(),
            new Pk1TuplePayload(os, hs, ls, cs, vs)
        );
    }
    
    /**
     * Get size of header section in bytes.
     * <p>
     * @param header - the header
     * @return number of bytes needed to store the header
     */
    public int getHeaderSectionSize(Pk1TupleHeader header) {
        return header.canStoreOhlcSizesInHeader() ? 3 : 3
            + byteUtils.intSize(header.openSize())
            + byteUtils.intSize(header.highSize())
            + byteUtils.intSize(header.lowSize())
            + byteUtils.intSize(header.closeSize());
    }
    
    /**
     * Get size of decimals section in bytes.
     * <p>
     * @param header - the header
     * @return number of bytes needed to store the number of decimals
     */
    public int getDecimalsSectionSize(Pk1TupleHeader header) {
        if ( header.canStoreNumberOfDecimalsInHeader() ) {
            return 0;
        }
        return byteUtils.intSize(header.decimals())
            + byteUtils.intSize(header.volumeDecimals());
    }
    
    /**
     * Get size of OHLC data section in bytes.
     * <p>
     * @param header - the header
     * @return number of bytes needed to store OHLC data
     */
    public int getOhlcDataSectionSize(Pk1TupleHeader header) {
        return header.openSize() + header.highSize() + header.lowSize() + header.closeSize();
    }
    
    /**
     * Get size of volume data section in bytes.
     * <p>
     * @param header - the header
     * @return number of bytes needed to store volume data
     */
    public int getVolumeDataSectionSize(Pk1TupleHeader header) {
        return header.volumeSize();
    }
    
    /**
     * Get total size of record described by header in bytes.
     * <p>
     * @param header - the header
     * @return number of bytes needed to store the record
     */
    public int getRecordSize(Pk1TupleHeader header) {
        return getHeaderSectionSize(header) + getDecimalsSectionSize(header)
            + getOhlcDataSectionSize(header) + getVolumeDataSectionSize(header);
    }
    
    public ByteBuffer newByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }
    
    public void packHeaderByte1(Pk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreNumberOfDecimalsInHeader() ) {
            dest.put((byte)(
                    byteUtils.boolToBit(!header.canStoreOhlcSizesInHeader(), 1) |
                    byteUtils.intToF3b(header.decimals(), 2) |
                    byteUtils.intToF3b(header.volumeDecimals(), 5)
                ));
        } else {
            dest.put((byte)(
                    byteUtils.boolToBit(true, 0) |
                    byteUtils.boolToBit(!header.canStoreOhlcSizesInHeader(), 1) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.decimals()), 2) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.volumeDecimals()), 5)
                ));
        }
    }
//    
//    public Pk1TupleHeaderByte1 unpackHeaderByte1(ByteBuffer source) {
//        byte b = source.get();
//        boolean canStoreNumberOfDecimalsInHeader = !byteUtils.bitToBool(b, 0),
//                canStoreOhlcSizesInHeader = !byteUtils.bitToBool(b, 1);
//        int decimals, volumeDecimals;
//        if ( canStoreNumberOfDecimalsInHeader ) {
//            decimals = byteUtils.f3bToInt(b, 2);
//            volumeDecimals = byteUtils.f3bToInt(b, 5);
//        } else {
//            decimals = byteUtils.f3bToSize(b, 2);
//            volumeDecimals = byteUtils.f3bToSize(b, 5);
//        }
//        return new Pk1TupleHeaderByte1(
//                canStoreNumberOfDecimalsInHeader,
//                canStoreOhlcSizesInHeader,
//                decimals,
//                volumeDecimals
//            );
//    }
    
    public void packHeaderOpenAndHigh(Pk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() ) {
            dest.put((byte)(
                    byteUtils.boolToBit(header.isHighRelative(), 0) |
                    byteUtils.sizeToF3b(header.highSize(), 1) |
                    byteUtils.sizeToF3b(header.openSize(), 5)
                ));
        } else {
            dest.put((byte)(
                    byteUtils.boolToBit(header.isHighRelative(), 0) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.highSize()), 1) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.openSize()), 5)
                ));
        }
    }
    
    public void packHeaderLowAndClose(Pk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() ) {
            dest.put((byte)(
                    byteUtils.boolToBit(header.isCloseRelative(), 0) |
                    byteUtils.sizeToF3b(header.closeSize(), 1) |
                    byteUtils.boolToBit(header.isLowRelative(), 4) |
                    byteUtils.sizeToF3b(header.lowSize(), 5)
                ));
        } else {
            dest.put((byte)(
                    byteUtils.boolToBit(header.isCloseRelative(), 0) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.closeSize()), 1) |
                    byteUtils.boolToBit(header.isLowRelative(), 4) |
                    byteUtils.sizeToF3b(byteUtils.intSize(header.lowSize()), 5)
                ));
        }
        
    }
    
    public void packHeaderOhlcSizes(Pk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreOhlcSizesInHeader() == false ) {
            // TODO: I would like to do it faster and cheaper
            dest.put(byteUtils.intToBytes(header.openSize()).copyBytes());
            dest.put(byteUtils.intToBytes(header.highSize()).copyBytes());
            dest.put(byteUtils.intToBytes(header.lowSize()).copyBytes());
            dest.put(byteUtils.intToBytes(header.closeSize()).copyBytes());
        }
    }
    
    public void packDecimals(Pk1TupleHeader header, ByteBuffer dest) {
        if ( header.canStoreNumberOfDecimalsInHeader() == false ) {
            // TODO: I would like to do it faster and cheaper
            dest.put(byteUtils.intToBytes(header.decimals()).copyBytes());
            dest.put(byteUtils.intToBytes(header.volumeDecimals()).copyBytes());
        }
    }
    
    public void packPayload(Pk1TuplePayload payload, ByteBuffer dest) {
        // TODO: I would like to do it faster and cheaper
        dest.put(payload.open().copyBytes());
        dest.put(payload.high().copyBytes());
        dest.put(payload.low().copyBytes());
        dest.put(payload.close().copyBytes());
        dest.put(payload.volume().copyBytes());
    }
    
}
