package ru.prolib.caelum.lib.kafka;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Bytes;

public class KafkaPackUtilsTest {
    private KafkaPackUtils service;

    @Before
    public void setUp() throws Exception {
        service = new KafkaPackUtils();
    }

    @Test
    public void testToTuplePk_AllAbsolute() {
        KafkaRawTuple tuple = new KafkaRawTuple(
                new Bytes(BigInteger.valueOf(17289L).toByteArray()),
                new Bytes(BigInteger.valueOf(5000009912435L).toByteArray()),
                new Bytes(BigInteger.valueOf(-98).toByteArray()),
                new Bytes(BigInteger.valueOf(-23).toByteArray()),
                4,
                new Bytes(BigInteger.valueOf(100000L).toByteArray()),
                6
            );
        
        KafkaTuplePk actual = service.toTuplePk(tuple);
        
        assertEquals(new KafkaTuplePk(
                new KafkaTuplePkHeaderBuilder()
                    .openSize(2)
                    .highRelative(false)
                    .highSize(6)
                    .lowRelative(false)
                    .lowSize(1)
                    .closeRelative(false)
                    .closeSize(1)
                    .decimals(4)
                    .volumeSize(3)
                    .volumeDecimals(6)
                    .build(),
                new KafkaTuplePkPayload(
                    new Bytes(BigInteger.valueOf(17289L).toByteArray()),
                    new Bytes(BigInteger.valueOf(5000009912435L).toByteArray()),
                    new Bytes(BigInteger.valueOf(-98).toByteArray()),
                    new Bytes(BigInteger.valueOf(-23).toByteArray()),
                    new Bytes(BigInteger.valueOf(100000L).toByteArray())
                )), actual);
    }

}
