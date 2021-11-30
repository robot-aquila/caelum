package ru.prolib.caelum.lib.data;

import java.math.BigInteger;
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;

import ru.prolib.caelum.lib.Bytes;

public class ItemData implements IItemData {
    private final BigInteger value;
    private final int decimals;
    private final BigInteger volume;
    private final int volumeDecimals;
    private final Bytes customData;
    
    public ItemData(BigInteger value,
            int decimals,
            BigInteger volume,
            int volumeDecimals,
            Bytes customData
    ) {
        this.value = value;
        this.decimals = decimals;
        this.volume = Objects.requireNonNull(volume, "Volume cannot be null");
        this.volumeDecimals = volumeDecimals;
        this.customData = customData;
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || !(other instanceof IItemData) ) {
            return false;
        }
        IItemData o = (IItemData) other;
        return new EqualsBuilder()
                .append(o.value(), value)
                .append(o.decimals(), decimals)
                .append(o.volume(), volume)
                .append(o.volumeDecimals(), volumeDecimals)
                .append(o.customData(), customData)
                .build();
    }

    @Override
    public BigInteger value() {
        return value;
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

    @Override
    public Bytes customData() {
        return customData;
    }
}
