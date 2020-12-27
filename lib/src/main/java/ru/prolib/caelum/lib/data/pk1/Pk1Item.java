package ru.prolib.caelum.lib.data.pk1;

import java.util.Objects;

/**
 * Structure to combine both Pk1 item header and data.
 */
record Pk1Item (IPk1ItemHeader header, Pk1ItemPayload payload) {
    
    public Pk1Item {
        Objects.requireNonNull(header);
        Objects.requireNonNull(payload);
    }
    
}
