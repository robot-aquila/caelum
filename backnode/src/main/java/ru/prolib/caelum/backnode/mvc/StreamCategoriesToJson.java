package ru.prolib.caelum.backnode.mvc;

import java.time.Clock;
import com.fasterxml.jackson.core.JsonFactory;
import ru.prolib.caelum.core.ICloseableIterator;

public class StreamCategoriesToJson extends StreamStringsToJson {

	public StreamCategoriesToJson(JsonFactory jsonFactory, ICloseableIterator<String> iterator, Clock clock) {
		super(jsonFactory, iterator, clock);
	}

}
