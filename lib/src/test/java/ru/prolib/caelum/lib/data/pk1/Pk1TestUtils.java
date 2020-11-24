package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;

import ru.prolib.caelum.lib.Bytes;

public class Pk1TestUtils {

    public static Pk1TupleHeaderBuilder tupleHeaderBuilderRandom() {
        return new Pk1TupleHeaderBuilder()
            .openSize(4)
            .highRelative(true)
            .highSize(1)
            .lowRelative(true)
            .lowSize(2)
            .closeRelative(true)
            .closeSize(1)
            .decimals(10)
            .volumeSize(2)
            .volumeDecimals(5);
    }
    
    public static Pk1TuplePayload tuplePayloadRandom() {
        var rnd = ThreadLocalRandom.current();
        return new Pk1TuplePayload(
                new Bytes(BigInteger.valueOf(rnd.nextLong()).toByteArray()),
                new Bytes(BigInteger.valueOf(rnd.nextLong()).toByteArray()),
                new Bytes(BigInteger.valueOf(rnd.nextLong()).toByteArray()),
                new Bytes(BigInteger.valueOf(rnd.nextLong()).toByteArray()),
                new Bytes(BigInteger.valueOf(rnd.nextLong()).toByteArray())
            );
    }

}
