package ru.prolib.caelum.lib.kafka;

import org.apache.kafka.common.serialization.Serializer;

import ru.prolib.caelum.lib.ByteUtils;
import ru.prolib.caelum.lib.Bytes;

public class KafkaRawTupleSerializer implements Serializer<KafkaRawTuple> {
	private final ByteUtils utils;
	
	public KafkaRawTupleSerializer(ByteUtils utils) {
		this.utils = utils;
	}

	@Override
	public byte[] serialize(String topic, KafkaRawTuple tuple) {
		// TODO: At now a relative form writing of value is not supported
		int decimals = tuple.getDecimals(), volumeDecimals = tuple.getVolumeDecimals();
		Bytes bytesOpen = tuple.getOpen(), bytesHigh = tuple.getHigh(), bytesLow = tuple.getLow(),
				bytesClose = tuple.getClose(), bytesVolume = tuple.getVolume();
		boolean hdrMpDcm, hdrMpOhlc;
		int lenTotal = 3, arrDecimalsLenSize = 0, arrVolumeDecimalsLenSize = 0,
				arrOpenLenSize = 0, arrHighLenSize = 0, arrLowLenSize = 0, arrCloseLenSize = 0;
		byte[] arrDecimalsLen = null, arrVolumeDecimalsLen = null, arrOpenLen = null,
				arrHighLen = null, arrLowLen = null, arrCloseLen = null;
		byte hdrByte1 = 0, hdrOpenHigh = 0, hdrLowClose = 0;
		if ( (hdrMpDcm = decimals > 7 || volumeDecimals > 7 || decimals < 0 || volumeDecimals < 0) ) {
			arrDecimalsLenSize = utils.intToByteArray(decimals, arrDecimalsLen = new byte[4]);
			arrVolumeDecimalsLenSize = utils.intToByteArray(volumeDecimals, arrVolumeDecimalsLen = new byte[4]);
			hdrByte1 |= (byte)0x01 | ((arrDecimalsLenSize - 1) << 2) | ((arrVolumeDecimalsLenSize - 1) << 5);
			lenTotal += arrDecimalsLenSize + arrVolumeDecimalsLenSize;
		} else {
			hdrByte1 |= (byte)decimals << 2 | (byte)volumeDecimals << 5;
		}
		if ( (hdrMpOhlc = bytesOpen.getLength() > 8 || bytesHigh.getLength() > 8
				|| bytesLow.getLength() > 8 || bytesClose.getLength() > 8) )
		{
			arrOpenLenSize = utils.intToByteArray(bytesOpen.getLength(), arrOpenLen = new byte[4]);
			arrHighLenSize = utils.intToByteArray(bytesHigh.getLength(), arrHighLen = new byte[4]);
			arrLowLenSize = utils.intToByteArray(bytesLow.getLength(), arrLowLen = new byte[4]);
			arrCloseLenSize = utils.intToByteArray(bytesClose.getLength(), arrCloseLen = new byte[4]);

			hdrOpenHigh |= ((arrOpenLenSize - 1) << 4) | ((arrHighLenSize - 1));
			lenTotal += arrOpenLenSize + arrHighLenSize + arrLowLenSize + arrCloseLenSize;
		}
		
		// TODO Auto-generated method stub
		return null;
	}

}
