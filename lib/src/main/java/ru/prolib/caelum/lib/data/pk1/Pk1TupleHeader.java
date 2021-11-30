package ru.prolib.caelum.lib.data.pk1;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.lib.ByteUtils;

public class Pk1TupleHeader implements IPk1TupleHeader {
    private final int decimals;
    private final int volumeDecimals;
    private final int openSize;
    private final boolean isHighRelative;
    private final int highSize;
    private final boolean isLowRelative;
    private final int lowSize;
    private final boolean isCloseRelative;
    private final int closeSize;
    private final int volumeSize;
    
    public Pk1TupleHeader(int decimals, int volumeDecimals, int openSize, boolean isHighRelative, int highSize,
            boolean isLowRelative, int lowSize, boolean isCloseRelative, int closeSize, int volumeSize)
    {
        this.decimals = decimals;
        this.volumeDecimals = volumeDecimals;
        this.openSize = openSize;
        this.isHighRelative = isHighRelative;
        this.highSize = highSize;
        this.isLowRelative = isLowRelative;
        this.lowSize = lowSize;
        this.isCloseRelative = isCloseRelative;
        this.closeSize = closeSize;
        this.volumeSize = volumeSize;
    }

    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return decimals >=0 && decimals <= 7 && volumeDecimals >= 0 && volumeDecimals <= 7;
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

    @Override
    public int decimals() {
        return decimals;
    }

    @Override
    public int volumeDecimals() {
        return volumeDecimals;
    }

    @Override
    public int openSize() {
        return openSize;
    }

    @Override
    public boolean isHighRelative() {
        return isHighRelative;
    }

    @Override
    public int highSize() {
        return highSize;
    }

    @Override
    public boolean isLowRelative() {
        return isLowRelative;
    }

    @Override
    public int lowSize() {
        return lowSize;
    }

    @Override
    public boolean isCloseRelative() {
        return isCloseRelative;
    }

    @Override
    public int closeSize() {
        return closeSize;
    }

    @Override
    public int volumeSize() {
        return volumeSize;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1TupleHeader.class ) {
            return false;
        }
        Pk1TupleHeader o = (Pk1TupleHeader) other;
        return new EqualsBuilder()
                .append(o.decimals, decimals)
                .append(o.volumeDecimals, volumeDecimals)
                .append(o.openSize, openSize)
                .append(o.isHighRelative, isHighRelative)
                .append(o.highSize, highSize)
                .append(o.isLowRelative, isLowRelative)
                .append(o.lowSize, lowSize)
                .append(o.isCloseRelative, isCloseRelative)
                .append(o.closeSize, closeSize)
                .append(o.volumeSize, volumeSize)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(decimals)
                .append(volumeDecimals)
                .append(openSize)
                .append(isHighRelative)
                .append(highSize)
                .append(isLowRelative)
                .append(lowSize)
                .append(isCloseRelative)
                .append(closeSize)
                .append(volumeSize)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("decimals", decimals)
                .append("volumeDecimals", volumeDecimals)
                .append("openSize", openSize)
                .append("isHighRelative", isHighRelative)
                .append("highSize", highSize)
                .append("isLowRelative", isLowRelative)
                .append("lowSize", lowSize)
                .append("isCloseRelative", isCloseRelative)
                .append("closeSize", closeSize)
                .append("volumeSize", volumeSize)
                .build();
    }
}
