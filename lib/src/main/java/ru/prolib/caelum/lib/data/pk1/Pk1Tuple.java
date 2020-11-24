package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

/**
 * OHLCV tuple ready to be packed into bytes or just unpacked from byte representation.
 */
record Pk1Tuple (Pk1TupleHeader header, Pk1TuplePayload payload) {
    
    public Pk1Tuple {
        Objects.requireNonNull(header);
        Objects.requireNonNull(payload);
    }
    
}
