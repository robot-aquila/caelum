package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.ByteUtils;

public record Pk1ItemHeader(
        int decimals,
        int volumeDecimals,
        int valueSize,
        int volumeSize,
        int customDataSize
    ) implements IPk1ItemHeader
{

    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return decimals >=0 && decimals <= 7 && volumeDecimals >= 0 && volumeDecimals <= 7;
    }
    
    @Override
    public boolean isValuePresent() {
        return valueSize > 0;
    }
    
    @Override
    public boolean isVolumePresent() {
        return volumeSize > 0;
    }
    
    @Override
    public boolean canStoreSizesInHeader() {
        return valueSize >= 0 && valueSize < 9 && volumeSize >= 0 && volumeSize < 9;
    }
    
    @Override
    public int headerSize() {
        int size = 2;
        if ( canStoreNumberOfDecimalsInHeader() == false ) {
            size += ByteUtils.intSize(decimals) + ByteUtils.intSize(volumeDecimals);
        }
        if ( canStoreSizesInHeader() == false ) {
            if ( isValuePresent() ) {
                size += ByteUtils.intSize(valueSize);
            }
            if ( isVolumePresent() ) {
                size += ByteUtils.intSize(volumeSize);
            }
        }
        return size;
    }
    
    @Override
    public int recordSize() {
        return headerSize() + valueSize + volumeSize + customDataSize;
    }
    
}
