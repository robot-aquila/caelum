package ru.prolib.caelum.lib.data.pk1;

import ru.prolib.caelum.lib.Bytes;

public record Pk1ItemPayload(
        Bytes value,
        Bytes volume,
        Bytes customData
    )
{

}
