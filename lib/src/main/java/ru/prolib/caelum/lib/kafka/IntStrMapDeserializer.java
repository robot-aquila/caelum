package ru.prolib.caelum.lib.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import ru.prolib.caelum.lib.ByteUtils;

public class IntStrMapDeserializer implements Deserializer<Map<Integer, String>> {
	
	@Override
	public int hashCode() {
		return 2009865103;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != IntStrMapDeserializer.class ) {
			return false;
		}
		return true;
	}

	@Override
	public Map<Integer, String> deserialize(String topic, byte[] data) {
		Map<Integer, String> result = new HashMap<>();
		int passed_len = 0, total_len = data.length;
		while ( passed_len < total_len ) {
			byte header = data[passed_len]; passed_len ++;
			int id_num_bytes = ((header & 0b00001110) >> 1) + 1,
				dsize_num_bytes = ((header & 0b1110000) >> 5) + 1;
			boolean delete = (header & 0b00000001) == 1;
			long id = ByteUtils.bytesToLong(data, passed_len, id_num_bytes); passed_len += id_num_bytes;
			if ( delete ) {
				result.put((int)id, null);
			} else {
				long dsize = ByteUtils.bytesToLong(data, passed_len, dsize_num_bytes); passed_len += dsize_num_bytes;
				String d = new String(data, passed_len, (int)dsize); passed_len += dsize;
				result.put((int)id, d); 
			}
		}
		return result;
	}

}
