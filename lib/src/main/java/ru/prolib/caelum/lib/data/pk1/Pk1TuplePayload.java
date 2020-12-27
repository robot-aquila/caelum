package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;

import java.util.Objects;

record Pk1TuplePayload (
		Bytes open,
		Bytes high,
		Bytes low,
		Bytes close,
		Bytes volume
	)
{
	public Pk1TuplePayload {
		Objects.requireNonNull(open, "Open bytes was not defined");
		Objects.requireNonNull(high, "High bytes was not defined");
		Objects.requireNonNull(low, "Low bytes was not defined");
		Objects.requireNonNull(close, "Close bytes was not defined");
		Objects.requireNonNull(volume, "Volume bytes was not defined");
	}

}
