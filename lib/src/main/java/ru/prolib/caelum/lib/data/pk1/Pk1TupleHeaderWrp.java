package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1TupleHeaderWrp implements IPk1TupleHeader {
    private final ByteUtils byteUtils;
    private final byte[] bytes;
    private final int startOffset;
    
    public Pk1TupleHeaderWrp(ByteUtils byteUtils, byte[] bytes, int startOffset) {
        this.byteUtils = byteUtils;
        this.bytes = bytes;
        this.startOffset = startOffset;
    }
    
    public ByteUtils getByteUtils() {
        return byteUtils;
    }
    
    public byte[] getBytes() {
        return bytes;
    }
    
    public int getStartOffset() {
        return startOffset;
    }

    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return !byteUtils.bitToBool(bytes[startOffset], 0);
    }

    @Override
    public boolean canStoreOhlcSizesInHeader() {
        return !byteUtils.bitToBool(bytes[startOffset], 1);
    }
    
    /**
     * Get offset of the first byte of the decimals section.
     * <p>
     * @return first byte offset including {@link #startOffset} of this object.
     * In other words that is absolute position of the section.
     */
    private int getDecimalsSectionStartOffset() {
        if ( canStoreOhlcSizesInHeader() ) {
            return startOffset + 3;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public int decimals() {
        if ( canStoreNumberOfDecimalsInHeader() ) {
            return byteUtils.f3bToInt(bytes[startOffset], 2);
        }
        return (int)
            byteUtils.bytesToLong(bytes, getDecimalsSectionStartOffset(), byteUtils.f3bToSize(bytes[startOffset], 2));
    }

    @Override
    public int volumeDecimals() {
        if ( canStoreNumberOfDecimalsInHeader() ) {
            return byteUtils.f3bToInt(bytes[startOffset], 5);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public int openSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isHighRelative() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int highSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isLowRelative() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int lowSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isCloseRelative() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int closeSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int volumeSize() {
        // TODO Auto-generated method stub
        return 0;
    }

}
