package ru.prolib.caelum.lib.data.pk1;

import java.math.BigInteger;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.lib.Bytes;
import ru.prolib.caelum.lib.data.ITupleData;

public class Pk1TupleData implements ITupleData {
    private final IPk1TupleHeader header;
    private final Bytes bytes;
    
    public Pk1TupleData(IPk1TupleHeader header, Bytes bytes) {
        this.header = header;
        this.bytes = bytes;
    }
    
    @Override
    public BigInteger open() {
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize(),
                header.openSize()
            );
    }

    @Override
    public BigInteger high() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize(),
                header.highSize()
            );
        return header.isHighRelative() ? open().subtract(x) : x;
    }

    @Override
    public BigInteger low() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize() + header.highSize(),
                header.lowSize()
            );
        return header.isLowRelative() ? open().subtract(x) : x;
    }

    @Override
    public BigInteger close() {
        var x = new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.headerSize() + header.openSize() + header.highSize() + header.lowSize(),
                header.closeSize()
            );
        return header.isCloseRelative() ? open().subtract(x) : x;
    }

    @Override
    public int decimals() {
        return header.decimals();
    }

    @Override
    public BigInteger volume() {
        int volumeSize = header.volumeSize();
        return new BigInteger(
                bytes.getSource(),
                bytes.getOffset() + header.recordSize() - volumeSize,
                volumeSize
            );
    }

    @Override
    public int volumeDecimals() {
        return header.volumeDecimals();
    }
    
    @Override
    public boolean equals(Object other) {
        if ( other == this ) {
            return true;
        }
        if ( other == null || other.getClass() != Pk1TupleData.class ) {
            return false;
        }
        var o = (Pk1TupleData) other;
        return new EqualsBuilder()
                .append(o.header, header)
                .append(o.bytes, bytes)
                .build();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(header)
                .append(bytes)
                .build();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("open", open())
                .append("high", high())
                .append("low", low())
                .append("close", close())
                .append("decimals", decimals())
                .append("volume", volume())
                .append("volumeDecimals", volumeDecimals())
                .build();
    }

}
