package ru.prolib.caelum.lib.kafka;

import java.util.Objects;

/**
 * OHLCV tuple ready to be packed into bytes or just unpacked from byte representation.
 */
record KafkaTuplePk (KafkaTuplePkHeader header, KafkaTuplePkPayload payload) {
    
    public KafkaTuplePk {
        Objects.requireNonNull(header);
        Objects.requireNonNull(payload);
    }
    
}
