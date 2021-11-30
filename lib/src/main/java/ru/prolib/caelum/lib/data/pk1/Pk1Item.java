package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Structure to combine both Pk1 item header and data.
 */
public class Pk1Item {
    private final IPk1ItemHeader header;
    private final Pk1ItemPayload payload;
    
    public Pk1Item(IPk1ItemHeader header, Pk1ItemPayload payload) {
        this.header = Objects.requireNonNull(header);
        this.payload = Objects.requireNonNull(payload);
    }
    
    public IPk1ItemHeader header() {
        return header;
    }
    
    public Pk1ItemPayload payload() {
        return payload;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1Item.class ) {
            return false;
        }
        Pk1Item o = (Pk1Item) other;
        return new EqualsBuilder()
                .append(o.header, header)
                .append(o.payload, payload)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(header)
                .append(payload)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("header", header)
                .append("payload", payload)
                .build();
    }
}
