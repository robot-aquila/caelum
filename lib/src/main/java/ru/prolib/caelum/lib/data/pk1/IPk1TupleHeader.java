package ru.prolib.caelum.lib.data.pk1;

public interface IPk1TupleHeader extends IPk1Header {
    
    /**
     * Test whether number of decimals can be stored in header or additional section should be used?
     * <p>
     * @return true if number of decimals can fit to header fields, false otherwise
     */
    boolean canStoreNumberOfDecimalsInHeader();
    
    /**
     * Test whether OHLC sizes can be stored in header or additional section should be used?
     * <p>
     * @return true if OHLC sizes can fit to header fields, false otherwise
     */
    boolean canStoreOhlcSizesInHeader();
    
    int decimals();
    int volumeDecimals();
    int openSize();
    boolean isHighRelative();
    int highSize();
    boolean isLowRelative();
    int lowSize();
    boolean isCloseRelative();
    int closeSize();
    int volumeSize();
}
