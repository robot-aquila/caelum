package ru.prolib.caelum.lib.data;

import java.math.BigInteger;
import java.util.Objects;

import org.apache.commons.lang3.builder.EqualsBuilder;

import ru.prolib.caelum.lib.Bytes;

public record ItemData (
        BigInteger value,
        int decimals,
        BigInteger volume,
        int volumeDecimals,
        Bytes customData) implements IItemData
{
    public ItemData {
        Objects.requireNonNull(volume, "Volume cannot be null");
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
}
