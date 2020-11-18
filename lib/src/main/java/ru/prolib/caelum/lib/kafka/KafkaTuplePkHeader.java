package ru.prolib.caelum.lib.kafka;

record KafkaTuplePkHeader (
		int decimals,
		int volumeDecimals,
		int openSize,
		boolean isHighRelative,
		int highSize,
		boolean isLowRelative,
		int lowSize,
		boolean isCloseRelative,
		int closeSize,
		int volumeSize
	)
{
	
}
