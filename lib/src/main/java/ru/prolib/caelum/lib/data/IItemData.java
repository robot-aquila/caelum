package ru.prolib.caelum.lib.data;

import java.math.BigInteger;

import ru.prolib.caelum.lib.Bytes;

public interface IItemData {
    
    /**
     * Get item value.
     * <p>
     * @return value or null if value not defined
     */
    BigInteger value();
    
    /**
     * Get number of decimals of value.
     * <p>
     * @return number of decimals
     */
    int decimals();
    
    /**
     * Get item volume.
     * <p>
     * @return volume. Volume cannot be null.
     */
    BigInteger volume();
    /**
     * Get volume number of decimals.
     * <p>
     * @return number of decimals
     */
    int volumeDecimals();
    
    /**
     * Get custom data.
     * <p>
     * @return custom data or null if custom data not defined
     */
    Bytes customData();
    
}
