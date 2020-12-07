package ru.prolib.caelum.lib.data;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;

public record TupleData (
        BigInteger open,
        BigInteger high,
        BigInteger low,
        BigInteger close,
        int decimals,
        BigInteger volume,
        int volumeDecimals) implements ITupleData
{
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || !(other instanceof ITupleData) ) {
            return false;
        }
        ITupleData o = (ITupleData) other;
        return new EqualsBuilder()
                .append(o.open(), open)
                .append(o.high(), high)
                .append(o.low(), low)
                .append(o.close(), close)
                .append(o.volume(), volume)
                .append(o.decimals(), decimals)
                .append(o.volumeDecimals(), volumeDecimals)
                .build();
    }
}
