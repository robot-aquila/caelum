package ru.prolib.caelum.lib.data;

import java.math.BigInteger;

public interface ITupleData {
    BigInteger open();
    BigInteger high();
    BigInteger low();
    BigInteger close();
    int decimals();
    BigInteger volume();
    int volumeDecimals();
}
