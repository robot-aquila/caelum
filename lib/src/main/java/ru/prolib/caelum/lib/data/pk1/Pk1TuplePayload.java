package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;

import static java.util.Objects.*;

record Pk1TuplePayload (
		Bytes open,
		Bytes high,
		Bytes low,
		Bytes close,
		Bytes volume
	)
{
	public Pk1TuplePayload {
		requireNonNull(open);
		requireNonNull(high);
		requireNonNull(low);
		requireNonNull(close);
		requireNonNull(volume);
	}

}
