package ru.prolib.caelum.lib.kafka;

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import ru.prolib.caelum.lib.ByteUtils;

public class IntStrMapSerializer implements Serializer<Map<Integer, String>> {
	private final ByteUtils utils;
	
	public IntStrMapSerializer(ByteUtils utils) {
		this.utils = utils;
	}
	
	public IntStrMapSerializer() {
		this(ByteUtils.getInstance());
	}
	
	@Override
	public int hashCode() {
		return 171899105;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != IntStrMapSerializer.class ) {
			return false;
		}
		return true;
	}

	@Override
	public byte[] serialize(String topic, Map<Integer, String> data) {
		byte records[][] = new byte[data.size()][];
		byte id_bytes[] = new byte[8], data_size_bytes[] = new byte[8];
		Iterator<Map.Entry<Integer, String>> it = data.entrySet().iterator();
		int i = 0, total_length = 0;
		while ( it.hasNext() ) {
			Map.Entry<Integer, String> entry = it.next();
			int event_id = entry.getKey();
			String event_data = entry.getValue();
			int id_num_bytes = utils.longToByteArray(event_id, id_bytes);
			byte record[] = null;
			if ( event_data == null ) {
				record = new byte[1 + id_num_bytes];
				record[0] = (byte)(0x01 | id_num_bytes - 1 << 1);
				System.arraycopy(id_bytes, 8 - id_num_bytes, record, 1, id_num_bytes);
			} else {
				byte data_bytes[] = event_data.getBytes();
				int data_size = data_bytes.length;
				int data_size_num_bytes = utils.longToByteArray(data_size, data_size_bytes);
				record = new byte[ 1 + id_num_bytes + data_size_num_bytes + data_size];
				record[0] = (byte)(id_num_bytes - 1 << 1 | data_size_num_bytes - 1 << 5);
				int used_length = 1;
				System.arraycopy(id_bytes, 8 - id_num_bytes, record, used_length, id_num_bytes);
				used_length += id_num_bytes;
				System.arraycopy(data_size_bytes, 8 - data_size_num_bytes, record, used_length, data_size_num_bytes);
				used_length += data_size_num_bytes;
				System.arraycopy(data_bytes, 0, record, used_length, data_size);
			}
			records[i] = record;
			total_length += record.length;
			i ++;
		}
		byte result[] = new byte[total_length];
		int used_length = 0;
		for ( i = 0; i < records.length; i ++ ) {
			byte record[] = records[i];
			System.arraycopy(record, 0, result, used_length, record.length);
			used_length += record.length;
		}
		return result;
	}

}
