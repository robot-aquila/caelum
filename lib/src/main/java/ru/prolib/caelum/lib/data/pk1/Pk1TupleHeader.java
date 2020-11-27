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
    ) implements IPk1TupleHeader
{

    @Override
    public boolean canStoreNumberOfDecimalsInHeader() {
        return decimals >=0 && decimals() <= 7 && volumeDecimals >= 0 && volumeDecimals <= 7;
    }
    
    @Override
    public boolean canStoreOhlcSizesInHeader() {
        return openSize > 0 && openSize < 9 && highSize > 0 && highSize < 9
            && lowSize > 0 && lowSize < 9 && closeSize > 0 && closeSize < 9;
    }
    
}
