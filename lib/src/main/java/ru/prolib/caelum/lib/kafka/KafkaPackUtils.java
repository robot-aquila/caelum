package ru.prolib.caelum.lib.kafka;

import java.math.BigInteger;

import ru.prolib.caelum.lib.Bytes;

public class KafkaPackUtils {

    public KafkaTuplePk toTuplePk(KafkaRawTuple tuple) {
        KafkaTuplePkHeaderBuilder headerBuilder = new KafkaTuplePkHeaderBuilder();
        Bytes os, hs, ls, cs, vs = tuple.getVolume();
        BigInteger
            o = new BigInteger((os = tuple.getOpen()).copyBytes()),
            h = new BigInteger((hs = tuple.getHigh()).copyBytes()),
            l = new BigInteger((ls = tuple.getLow()).copyBytes()),
            c = new BigInteger((cs = tuple.getClose()).copyBytes());
        Bytes
            hx = new Bytes(o.subtract(h).toByteArray()),
            lx = new Bytes(o.subtract(l).toByteArray()),
            cx = new Bytes(o.subtract(c).toByteArray());
        if ( hx.getLength() < hs.getLength() ) {
            hs = hx;
            headerBuilder.highRelative(true);
        } else {
            headerBuilder.highRelative(false);
        }
        if ( lx.getLength() < ls.getLength() ) {
            ls = lx;
            headerBuilder.lowRelative(true);
        } else {
            headerBuilder.lowRelative(false);
        }
        if ( cx.getLength() < cs.getLength() ) {
            cs = cx;
            headerBuilder.closeRelative(true);
        } else {
            headerBuilder.closeRelative(false);
        }
        return new KafkaTuplePk(headerBuilder
                .openSize(os.getLength())
                .highSize(hs.getLength())
                .lowSize(ls.getLength())
                .closeSize(cs.getLength())
                .volumeSize(vs.getLength())
                .decimals(tuple.getDecimals())
                .volumeDecimals(tuple.getVolumeDecimals())
                .build(),
            new KafkaTuplePkPayload(os, hs, ls, cs, vs)
        );
    }

}
