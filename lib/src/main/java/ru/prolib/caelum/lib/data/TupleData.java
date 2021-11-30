package ru.prolib.caelum.lib.data;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class TupleData implements ITupleData {
    private final BigInteger open;
    private final BigInteger high;
    private final BigInteger low;
    private final BigInteger close;
    private final int decimals;
    private final BigInteger volume;
    private final int volumeDecimals;
    
    public TupleData(BigInteger open,
        BigInteger high,
        BigInteger low,
        BigInteger close,
        int decimals,
        BigInteger volume,
        int volumeDecimals
    ) {
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.decimals = decimals;
        this.volume = volume;
        this.volumeDecimals = volumeDecimals;
    }
    
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

    @Override
    public BigInteger open() {
        return open;
    }

    @Override
    public BigInteger high() {
        return high;
    }

    @Override
    public BigInteger low() {
        return low;
    }

    @Override
    public BigInteger close() {
        return close;
    }

    @Override
    public int decimals() {
        return decimals;
    }

    @Override
    public BigInteger volume() {
        return volume;
    }

    @Override
    public int volumeDecimals() {
        return volumeDecimals;
    }
}
