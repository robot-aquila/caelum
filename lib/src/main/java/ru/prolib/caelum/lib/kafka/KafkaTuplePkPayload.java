package ru.prolib.caelum.lib.kafka;

import ru.prolib.caelum.lib.Bytes;

import static java.util.Objects.*;

record KafkaTuplePkPayload (
		Bytes open,
		Bytes high,
		Bytes low,
		Bytes close,
		Bytes volume
	)
{
	public KafkaTuplePkPayload {
		requireNonNull(open);
		requireNonNull(high);
		requireNonNull(low);
		requireNonNull(close);
		requireNonNull(volume);
	}

}
