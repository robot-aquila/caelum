package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1TupleHeaderWrp implements IPk1TupleHeader {
    private final byte[] bytes;
    private final int startOffset, recordSize;
    
    public Pk1TupleHeaderWrp(byte[] bytes, int startOffset, int recordSize) {
        this.bytes = bytes;
        this.startOffset = startOffset;
        this.recordSize = recordSize;
    }
    
    public byte[] getBytes() {
        return bytes;
    }
    
    public int getStartOffset() {
        return startOffset;
    }
    
    @Override
    public int recordSize() {
        return recordSize;
    }
    
    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return !ByteUtils.bitToBool(bytes[startOffset], 0);
    }

    @Override
    public boolean canStoreOhlcSizesInHeader() {
        return !ByteUtils.bitToBool(bytes[startOffset], 1);
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
        byte b1 = bytes[startOffset + 1], b2 = bytes[startOffset + 2]; 
        return startOffset + 3
                + ByteUtils.f3bToSize(b1, 1)
                + ByteUtils.f3bToSize(b1, 5)
                + ByteUtils.f3bToSize(b2, 1)
                + ByteUtils.f3bToSize(b2, 5);
    }

    @Override
    public int decimals() {
        if ( canStoreNumberOfDecimalsInHeader() ) {
            return ByteUtils.f3bToInt(bytes[startOffset], 2);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                getDecimalsSectionStartOffset(),
                ByteUtils.f3bToSize(bytes[startOffset], 2)
            );
    }

    @Override
    public int volumeDecimals() {
        if ( canStoreNumberOfDecimalsInHeader() ) {
            return ByteUtils.f3bToInt(bytes[startOffset], 5);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                getDecimalsSectionStartOffset() + ByteUtils.f3bToSize(bytes[startOffset], 2),
                ByteUtils.f3bToSize(bytes[startOffset], 5)
            );
    }

    @Override
    public int openSize() {
        if ( canStoreOhlcSizesInHeader() ) {
            return ByteUtils.f3bToSize(bytes[startOffset + 1], 5);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 3,
                ByteUtils.f3bToSize(bytes[startOffset + 1], 5)
            );
    }

    @Override
    public boolean isHighRelative() {
        return ByteUtils.bitToBool(bytes[startOffset + 1], 0);
    }

    @Override
    public int highSize() {
        if ( canStoreOhlcSizesInHeader() ) {
            return ByteUtils.f3bToSize(bytes[startOffset + 1], 1);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 3 + ByteUtils.f3bToSize(bytes[startOffset + 1], 5),
                ByteUtils.f3bToSize(bytes[startOffset + 1], 1)
            );
    }

    @Override
    public boolean isLowRelative() {
        return ByteUtils.bitToBool(bytes[startOffset + 2], 4);
    }

    @Override
    public int lowSize() {
        if ( canStoreOhlcSizesInHeader() ) {
            return ByteUtils.f3bToSize(bytes[startOffset + 2], 5);
        }
        byte b1 = bytes[startOffset + 1];
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 3 + ByteUtils.f3bToSize(b1, 1) + ByteUtils.f3bToSize(b1, 5),
                ByteUtils.f3bToSize(bytes[startOffset + 2], 5)
            );
    }

    @Override
    public boolean isCloseRelative() {
        return ByteUtils.bitToBool(bytes[startOffset + 2], 0);
    }

    @Override
    public int closeSize() {
        if ( canStoreOhlcSizesInHeader() ) {
            return ByteUtils.f3bToSize(bytes[startOffset + 2], 1);
        }
        byte b1 = bytes[startOffset + 1], b2 = bytes[startOffset + 2];
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 3 + ByteUtils.f3bToSize(b1, 1) + ByteUtils.f3bToSize(b1, 5) + ByteUtils.f3bToSize(b2, 5),
                ByteUtils.f3bToSize(b2, 1)
            );
    }
    
    @Override
    public int headerSize() {
        int size = 3;
        if ( canStoreNumberOfDecimalsInHeader() == false ) {
            size += ByteUtils.f3bToSize(bytes[startOffset], 2) + ByteUtils.f3bToSize(bytes[startOffset], 5);
        }
        if ( canStoreOhlcSizesInHeader() == false ) {
            byte b1 = bytes[startOffset + 1], b2 = bytes[startOffset + 2];
            size += ByteUtils.f3bToSize(b1, 1) + ByteUtils.f3bToSize(b1, 5)
                  + ByteUtils.f3bToSize(b2, 1) + ByteUtils.f3bToSize(b2, 5);
        }
        return size;
    } 
    
    
    @Override
    public int volumeSize() {
        return recordSize - headerSize() - openSize() - highSize() - lowSize() - closeSize();
    }

}
