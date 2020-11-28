package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.ByteUtils;

record Pk1TupleHeader (
        int decimals,
        int volumeDecimals,
        int openSize,
        boolean isHighRelative,
        int highSize,
        boolean isLowRelative,
        int lowSize,
        boolean isCloseRelative,
        int closeSize,
        int volumeSize
    ) implements IPk1TupleHeader
{

    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return decimals >=0 && decimals() <= 7 && volumeDecimals >= 0 && volumeDecimals <= 7;
    }
    
    @Override
    public boolean canStoreOhlcSizesInHeader() {
        return openSize > 0 && openSize < 9 && highSize > 0 && highSize < 9
            && lowSize > 0 && lowSize < 9 && closeSize > 0 && closeSize < 9;
    }
    
    @Override
    public int headerSize() {
        int size = 3;
        if ( canStoreNumberOfDecimalsInHeader() == false ) {
            size += ByteUtils.intSize(decimals) + ByteUtils.intSize(volumeDecimals);
        }
        if ( canStoreOhlcSizesInHeader() == false ) {
            size += ByteUtils.intSize(openSize) + ByteUtils.intSize(highSize)
                + ByteUtils.intSize(lowSize) + ByteUtils.intSize(closeSize);
        }
        return size;
    }
    
    @Override
    public int recordSize() {
        return headerSize() + openSize + highSize + lowSize + closeSize + volumeSize;
    }
    
}
