package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1ItemHeaderWrp implements IPk1ItemHeader {
    private final byte[] bytes;
    private final int startOffset, recordSize;
    
    public Pk1ItemHeaderWrp(byte[] bytes, int startOffset, int recordSize) {
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
    public boolean canStoreSizesInHeader() {
        return !ByteUtils.bitToBool(bytes[startOffset], 1);
    }
    
    private int getDecimalsSectionStartOffset() {
        if ( canStoreSizesInHeader() ) {
            return startOffset + 2;
        }
        byte b1 = bytes[startOffset + 1];
        return startOffset + 2
                + (ByteUtils.bitToBool(b1, 0) ? ByteUtils.f3bToSize(b1, 1) : 0)
                + (ByteUtils.bitToBool(b1, 4) ? ByteUtils.f3bToSize(b1, 5) : 0);
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
    public boolean isValuePresent() {
        return ByteUtils.bitToBool(bytes[startOffset + 1], 4);
    }

    @Override
    public boolean isVolumePresent() {
        return ByteUtils.bitToBool(bytes[startOffset + 1], 0);
    }

    @Override
    public int valueSize() {
        if ( isValuePresent() == false ) return 0;
        if ( canStoreSizesInHeader() ) {
            return ByteUtils.f3bToSize(bytes[startOffset + 1], 5);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 2,
                ByteUtils.f3bToSize(bytes[startOffset + 1], 5)
            );
    }

    @Override
    public int volumeSize() {
        if ( isVolumePresent() == false ) return 0;
        byte b1 = bytes[startOffset + 1];
        if ( canStoreSizesInHeader() ) {
            return ByteUtils.f3bToSize(b1, 1);
        }
        return (int) ByteUtils.bytesToLong(
                bytes,
                startOffset + 2 + (isValuePresent() ? ByteUtils.f3bToSize(b1, 5) : 0),
                ByteUtils.f3bToSize(b1, 1)
            );
    }
    

    @Override
    public int headerSize() {
        int size = 2;
        if ( canStoreNumberOfDecimalsInHeader() == false ) {
            byte b0 = bytes[startOffset];
            size += ByteUtils.f3bToSize(b0, 2) + ByteUtils.f3bToSize(b0, 5);
        }
        if ( canStoreSizesInHeader() == false ) {
            byte b1 = bytes[startOffset + 1];
            if ( isVolumePresent() ) {
                size += ByteUtils.f3bToSize(b1, 1);
            }
            if ( isValuePresent() ) {
                size += ByteUtils.f3bToSize(b1, 5);
            }
        }
        return size;
    }

    @Override
    public int customDataSize() {
        return recordSize - headerSize() - valueSize() - volumeSize();
    }
    
}
