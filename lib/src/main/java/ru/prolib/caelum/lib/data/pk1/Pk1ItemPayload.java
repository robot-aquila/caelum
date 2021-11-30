package ru.prolib.caelum.lib.data.pk1;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.lib.Bytes;

public class Pk1ItemPayload {
    private final Bytes value;
    private final Bytes volume;
    private final Bytes customData;

    public Pk1ItemPayload(Bytes value, Bytes volume, Bytes customData ){
        this.value = value;
        this.volume = volume;
        this.customData = customData;
    }

    public Bytes value() {
        return value;
    }

    public Bytes volume() {
        return volume;
    }

    public Bytes customData() {
        return customData;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1ItemPayload.class ) {
            return false;
        }
        Pk1ItemPayload o = (Pk1ItemPayload) other;
        return new EqualsBuilder()
                .append(o.value, value)
                .append(o.volume, volume)
                .append(o.customData, customData)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(value)
                .append(volume)
                .append(customData)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("value", value)
                .append("volume", volume)
                .append("customData", customData)
                .build();
    }
}
