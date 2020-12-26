package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Pk1TupleHeaderBuilder {
    private Integer decimals, volumeDecimals, openSize, highSize, lowSize, closeSize, volumeSize;
    private Boolean highRelative, lowRelative, closeRelative;
    
    public Pk1TupleHeaderBuilder decimals(int decimals) {
        this.decimals = decimals;
        return this;
    }
    
    public Pk1TupleHeaderBuilder volumeDecimals(int decimals) {
        this.volumeDecimals = decimals;
        return this;
    }
    
    public Pk1TupleHeaderBuilder openSize(int size) {
        this.openSize = size;
        return this;
    }
    
    public Pk1TupleHeaderBuilder highSize(int size) {
        this.highSize = size;
        return this;
    }
    
    public Pk1TupleHeaderBuilder lowSize(int size) {
        this.lowSize = size;
        return this;
    }
    
    public Pk1TupleHeaderBuilder closeSize(int size) {
        this.closeSize = size;
        return this;
    }
    
    public Pk1TupleHeaderBuilder volumeSize(int size) {
        this.volumeSize = size;
        return this;
    }
    
    public Pk1TupleHeaderBuilder highRelative(boolean relative) {
        this.highRelative = relative;
        return this;
    }
    
    public Pk1TupleHeaderBuilder lowRelative(boolean relative) {
        this.lowRelative = relative;
        return this;
    }
    
    public Pk1TupleHeaderBuilder closeRelative(boolean relative) {
        this.closeRelative = relative;
        return this;
    }
    
    public Pk1TupleHeader build() {
        Objects.requireNonNull(decimals, "Decimals was not specified");
        Objects.requireNonNull(volumeDecimals, "Volume decimals was not specified");
        Objects.requireNonNull(openSize, "Size of opening value component was not specified");
        Objects.requireNonNull(highSize, "Size of highest value component was not specified");
        Objects.requireNonNull(lowSize, "Size of lowest value component was not specified");
        Objects.requireNonNull(closeSize, "Size of closing value component was not specified");
        Objects.requireNonNull(volumeSize, "Size of volume component was not specified");
        Objects.requireNonNull(highRelative, "Relativity of the high value component was not specified");
        Objects.requireNonNull(lowRelative, "Relativity of the low value component was not specified");
        Objects.requireNonNull(closeRelative, "Relativity of the close value component was not specified");
        return new Pk1TupleHeader(
                decimals,
                volumeDecimals,
                openSize,
                highRelative,
                highSize,
                lowRelative,
                lowSize,
                closeRelative,
                closeSize,
                volumeSize
            );
    }
    
    @Override
    public boolean equals(Object other) {
        if  ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1TupleHeaderBuilder.class ) {
            return false;
        }
        Pk1TupleHeaderBuilder o = (Pk1TupleHeaderBuilder) other;
        return new EqualsBuilder()
                .append(o.decimals, decimals)
                .append(o.volumeDecimals, volumeDecimals)
                .append(o.openSize, openSize)
                .append(o.highRelative, highRelative)
                .append(o.highSize, highSize)
                .append(o.lowRelative, lowRelative)
                .append(o.lowSize, lowSize)
                .append(o.closeRelative, closeRelative)
                .append(o.closeSize, closeSize)
                .append(o.volumeSize, volumeSize)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("decimals", decimals)
                .append("volumeDecimals", volumeDecimals)
                .append("openSize", openSize)
                .append("highRelative", highRelative)
                .append("highSize", highSize)
                .append("lowRelative", lowRelative)
                .append("lowSize", lowSize)
                .append("closeRelative", closeRelative)
                .append("closeSize", closeSize)
                .append("volumeSize", volumeSize)
                .build();
    }

}
