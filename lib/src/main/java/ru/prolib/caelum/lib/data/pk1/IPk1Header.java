package ru.prolib.caelum.lib.data.pk1;

public interface IPk1Header {
    /**
     * Get record total size in bytes.
     * <p>
     * @return number of bytes
     */
    int recordSize();
    
    /**
     * Get header size in bytes.
     * <p>
     * @return number of bytes
     */
    int headerSize();
}
