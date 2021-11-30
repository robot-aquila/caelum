package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Pk1TuplePayload  {
    private final Bytes open;
    private final Bytes high;
    private final Bytes low;
    private final Bytes close;
    private final Bytes volume;
    
    public Pk1TuplePayload(Bytes open, Bytes high, Bytes low, Bytes close, Bytes volume) {
        this.open = Objects.requireNonNull(open, "Open bytes was not defined");
        this.high = Objects.requireNonNull(high, "High bytes was not defined");
        this.low = Objects.requireNonNull(low, "Low bytes was not defined");
        this.close = Objects.requireNonNull(close, "Close bytes was not defined");
        this.volume = Objects.requireNonNull(volume, "Volume bytes was not defined");
    }
    
    public Bytes open() {
        return open;
    }
    
    public Bytes high() {
        return high;
    }
    
    public Bytes low() {
        return low;
    }
    
    public Bytes close() {
        return close;
    }
    
    public Bytes volume() {
        return volume;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1TuplePayload.class ) {
            return false;
        }
        Pk1TuplePayload o = (Pk1TuplePayload) other;
        return new EqualsBuilder()
                .append(o.open, open)
                .append(o.high, high)
                .append(o.low, low)
                .append(o.close, close)
                .append(o.volume, volume)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(open)
                .append(high)
                .append(low)
                .append(close)
                .append(volume)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("open", open)
                .append("high", high)
                .append("low", low)
                .append("close", close)
                .append("volume", volume)
                .build();
    }
}
