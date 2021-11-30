package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Structure to combine both Pk1 tuple header and data.
 */
public class Pk1Tuple {
    private final IPk1TupleHeader header;
    private final Pk1TuplePayload payload;
    
    public Pk1Tuple(IPk1TupleHeader header, Pk1TuplePayload payload) {
        this.header = Objects.requireNonNull(header);
        this.payload = Objects.requireNonNull(payload);
    }
    
    public IPk1TupleHeader header() {
        return header;
    }
    
    public Pk1TuplePayload payload() {
        return payload;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1Tuple.class ) {
            return false;
        }
        Pk1Tuple o = (Pk1Tuple) other;
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
