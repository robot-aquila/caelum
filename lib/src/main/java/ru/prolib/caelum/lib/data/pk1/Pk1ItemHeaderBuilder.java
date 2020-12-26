package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Pk1ItemHeaderBuilder {
    private Integer decimals, volumeDecimals, valueSize, volumeSize, customDataSize;
    
    public Pk1ItemHeaderBuilder decimals(int decimals) {
        this.decimals = decimals;
        return this;
    }
    
    public Pk1ItemHeaderBuilder volumeDecimals(int decimals) {
        this.volumeDecimals = decimals;
        return this;
    }
    
    public Pk1ItemHeaderBuilder valueSize(int size) {
        this.valueSize = size;
        return this;
    }
    
    public Pk1ItemHeaderBuilder volumeSize(int size) {
        this.volumeSize = size;
        return this;
    }
    
    public Pk1ItemHeaderBuilder customDataSize(int size) {
        this.customDataSize = size;
        return this;
    }
    
    public Pk1ItemHeader build() {
        Objects.requireNonNull(decimals, "Decimals was not specified");
        Objects.requireNonNull(volumeDecimals, "Volume decimals was not specified");
        Objects.requireNonNull(valueSize, "Value size was not specified");
        Objects.requireNonNull(volumeSize, "Volume size was not specified");
        Objects.requireNonNull(customDataSize, "Custom data size was not specified");
        return new Pk1ItemHeader(decimals, volumeDecimals, valueSize, volumeSize, customDataSize);
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1ItemHeaderBuilder.class ) {
            return false;
        }
        Pk1ItemHeaderBuilder o = (Pk1ItemHeaderBuilder) other;
        return new EqualsBuilder()
                .append(o.decimals, decimals)
                .append(o.volumeDecimals, volumeDecimals)
                .append(o.valueSize, valueSize)
                .append(o.volumeSize, volumeSize)
                .append(o.customDataSize, customDataSize)
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
