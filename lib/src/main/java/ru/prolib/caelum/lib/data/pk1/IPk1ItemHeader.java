package ru.prolib.caelum.lib.data.pk1;

public interface IPk1ItemHeader extends IPk1Header {
    /**
     * Test whether number of decimals can be stored in header or additional section should be used?
     * <p>
     * @return true if number of decimals can fit to header fields, false otherwise
     */
    boolean canStoreNumberOfDecimalsInHeader();
    
    /**
     * Test whether value & volume sizes can be stored in header or additional section should be used?
     * <p>
     * @return true if sizes can fit to header fields, false otherwise
     */
    boolean canStoreSizesInHeader();
    
    int decimals();
    int volumeDecimals();
    boolean isValuePresent();
    boolean isVolumePresent();
    int valueSize();
    int volumeSize();
    int customDataSize();
}
