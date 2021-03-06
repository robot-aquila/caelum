package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

/**
 * Structure to combine both Pk1 tuple header and data.
 */
record Pk1Tuple (IPk1TupleHeader header, Pk1TuplePayload payload) {
    
    public Pk1Tuple {
        Objects.requireNonNull(header);
        Objects.requireNonNull(payload);
    }
    
}
