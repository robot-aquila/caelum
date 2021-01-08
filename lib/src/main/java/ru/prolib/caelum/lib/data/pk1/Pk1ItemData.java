package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.IItemData;

public class Pk1ItemData implements IItemData {
    private final IPk1ItemHeader header;
    private final Bytes bytes;
    
    public Pk1ItemData(IPk1ItemHeader header, Bytes bytes) {
        this.header = header;
        this.bytes = bytes;
    }
    
    @Override
    public BigInteger value() {
        if ( header.isValuePresent() == false ) {
            return null;
        }
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize(),
                header.valueSize()
            );
    }
    
    @Override
    public int decimals() {
        return header.decimals();
    }
    
    @Override
    public BigInteger volume() {
        if ( header.isVolumePresent() == false ) {
            return BigInteger.ZERO;
        }
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.valueSize(),
                header.volumeSize()
            );
    }
    
    @Override
    public int volumeDecimals() {
        return header.volumeDecimals();
    }
    
    @Override
    public Bytes customData() {
        int customDataSize = header.customDataSize();
        if ( customDataSize == 0 ) return null;
        int startOffset = header.recordSize() - customDataSize + bytes.getOffset();
        return new Bytes(bytes.getSource(), startOffset, customDataSize);
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1ItemData.class ) {
            return false;
        }
        var o = (Pk1ItemData) other;
        return new EqualsBuilder()
                .append(o.header, header)
                .append(o.bytes, bytes)
                .build();
    }
}
