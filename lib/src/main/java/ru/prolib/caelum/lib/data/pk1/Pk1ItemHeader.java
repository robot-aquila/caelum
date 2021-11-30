package ru.prolib.caelum.lib.data.pk1;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1ItemHeader implements IPk1ItemHeader {
    private final int decimals;
    private final int volumeDecimals;
    private final int valueSize;
    private final int volumeSize;
    private final int customDataSize;

    public Pk1ItemHeader(int decimals, int volumeDecimals, int valueSize, int volumeSize, int customDataSize) {
        this.decimals = decimals;
        this.volumeDecimals = volumeDecimals;
        this.valueSize = valueSize;
        this.volumeSize = volumeSize;
        this.customDataSize = customDataSize;
    }
    
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

    @Override
    public int decimals() {
        return decimals;
    }

    @Override
    public int volumeDecimals() {
        return volumeDecimals;
    }

    @Override
    public int valueSize() {
        return valueSize;
    }

    @Override
    public int volumeSize() {
        return volumeSize;
    }

    @Override
    public int customDataSize() {
        return customDataSize;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1ItemHeader.class ) {
            return false;
        }
        Pk1ItemHeader o = (Pk1ItemHeader) other;
        return new EqualsBuilder()
                .append(o.decimals, decimals)
                .append(o.volumeDecimals, volumeDecimals)
                .append(o.valueSize, valueSize)
                .append(o.volumeSize, volumeSize)
                .append(o.customDataSize, customDataSize)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(decimals)
                .append(volumeDecimals)
                .append(valueSize)
                .append(volumeSize)
                .append(customDataSize)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("decimals", decimals)
                .append("volumeDecimals", volumeDecimals)
                .append("valueSize", valueSize)
                .append("volumeSize", volumeSize)
                .append("customDataSize", customDataSize)
                .build();
    }
}
