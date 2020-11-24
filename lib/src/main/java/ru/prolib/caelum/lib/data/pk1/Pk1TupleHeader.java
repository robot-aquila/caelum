package ru.prolib.caelum.lib.data.pk1;

record Pk1TupleHeader (
        int decimals,
        int volumeDecimals,
        int openSize,
        boolean isHighRelative,
        int highSize,
        boolean isLowRelative,
        int lowSize,
        boolean isCloseRelative,
        int closeSize,
        int volumeSize
    )
{

}
