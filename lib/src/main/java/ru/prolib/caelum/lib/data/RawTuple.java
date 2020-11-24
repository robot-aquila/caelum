package ru.prolib.caelum.lib.data;

import static ru.prolib.caelum.lib.ByteUtils.bytesToHexString;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import ru.prolib.caelum.lib.Bytes;

public record RawTuple (
			Bytes open,
			Bytes high,
			Bytes low,
			Bytes close,
			int decimals,
			Bytes volume,
			int volumeDecimals)
{
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("open", bytesToHexString(open))
				.append("high", bytesToHexString(high))
				.append("low", bytesToHexString(low))
				.append("close", bytesToHexString(close))
				.append("decimals", decimals)
				.append("volume", bytesToHexString(volume))
				.append("volumeDecimals", volumeDecimals)
				.build();
	}
	
}
