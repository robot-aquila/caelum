package ru.prolib.caelum.lib.data;

import java.math.BigInteger;

import ru.prolib.caelum.lib.Bytes;

public interface IItemData {
    BigInteger value();
    int decimals();
    BigInteger volume();
    int volumeDecimals();
    Bytes customData();
}
