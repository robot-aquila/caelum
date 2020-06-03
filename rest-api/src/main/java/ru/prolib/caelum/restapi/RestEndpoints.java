package ru.prolib.caelum.restapi;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.core.IKafkaStreamsRegistry;
import ru.prolib.caelum.core.LBOHLCVMutable;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class RestEndpoints implements IKafkaStreamsRegistry {
	static final Logger logger = LoggerFactory.getLogger(RestEndpoints.class);
	
	static class StoreDesc {
		final String period, storeName;
		final KafkaStreams streams;
		
		public StoreDesc(String period, String store_name, KafkaStreams streams) {
			this.period = period;
			this.storeName = store_name;
			this.streams = streams;
		}
		
	}
	
	private final ConcurrentHashMap<String, StoreDesc> periodToStoreDesc = new ConcurrentHashMap<>();
	private final HostInfo hostInfo;
	private final JsonFactory jsonFactory = new JsonFactory();
	
	public RestEndpoints(HostInfo host_info) {
		this.hostInfo = host_info;
	}

	@Override
	public void registerOHLCVAggregator(String period, String store_name, KafkaStreams streams) {
		logger.info("Registered streams for store {} and period {}", store_name, period);
		periodToStoreDesc.put(period, new StoreDesc(period, store_name, streams));
	}
	
	public static class ProcessorMetadata {
		private final String host;
		private final int port;
		private List<Integer> topicPartitions;
		
		public ProcessorMetadata(String host, int port, List<Integer> topicPartitions) {
			this.host = host;
			this.port = port;
			this.topicPartitions = topicPartitions;
		}
		
		public String getHost() {
			return host;
		}
		
		public int getPort() {
			return port;
		}
		
		public List<Integer> getTopicPartitions() {
			return topicPartitions;
		}
		
	}
	
//	ReadOnlyWindowStore<String, LBCandleMutable> store = streams
//			.store(StoreQueryParameters.fromNameAndType(conf.getStoreName(), QueryableStoreTypes.windowStore()));
//	try ( KeyValueIterator<Windowed<String>, LBCandleMutable> it = store.all() ) {
//		while ( it.hasNext() ) {
//			KeyValue<Windowed<String>, LBCandleMutable> item = it.next();
//			Map<String, Object> map = new LinkedHashMap<>();
//			map.put("key", item.key.key());
//			map.put("time", item.key.window().startTime());
//			map.put("val", item.value);
//			System.out.println(map);
//		}
//	}
	
	@GET
	@Path("/ping")
	public Result<Void> ping() {
		return new Result<Void>(System.currentTimeMillis(), null);
	}
	
	@GET
	@Path("/ohlcv/{period}/{symbol}")
	public Response ohlcv(
			@PathParam("period") final String period,
			
			@PathParam("symbol") final String symbol,
			
			@QueryParam("from") Long from,
			
			@QueryParam("to") Long to,
			
			@DefaultValue("500")
			@QueryParam("limit") long limit)
	{
		if ( from == null ) {
			from = 0L;
		}
		if ( to == null ) {
			to = Long.MAX_VALUE;
		}
		if ( to <= from ) {
			// TODO: handle error
			to = Long.MAX_VALUE;
		}

		StoreDesc desc = periodToStoreDesc.get(period);
		if ( desc == null ) {
			// TODO: try to rebuild sequence from lower periods
			throw new NotFoundException();
		}
		final ReadOnlyWindowStore<String, LBOHLCVMutable> store = desc.streams
				.store(StoreQueryParameters.fromNameAndType(desc.storeName, QueryableStoreTypes.windowStore()));
		if ( store == null ) {
			throw new NotFoundException();
		}

		return Response.status(200)
				.entity(new JsonOHLCVStreamer(jsonFactory, store, symbol, period, from, to, limit))
				.build();
	}
	
	@GET
	@Path("/ohlcv/processors/{period}")
	public List<ProcessorMetadata> processors(@PathParam("period") final String period) {
		StoreDesc desc = periodToStoreDesc.get(period);
		if ( desc == null ) {
			throw new NotFoundException();
		}
		List<String> keys = new ArrayList<>();
		Enumeration<String> keys_enum = periodToStoreDesc.keys();
		while ( keys_enum.hasMoreElements() ) {
			keys.add(keys_enum.nextElement());
		}
		logger.info("Keys: {}", keys);
		for ( StreamsMetadata md : desc.streams.allMetadataForStore(desc.storeName) ) {
			logger.info("Metadata: host={} port={}", md.host(), md.port());
		}
		return desc.streams.allMetadataForStore(desc.storeName).stream()
			.map(md -> new ProcessorMetadata(md.host(), md.port(), md.topicPartitions().stream()
					.map(TopicPartition::partition)
					.collect(Collectors.toList()))
			)
			.collect(Collectors.toList());
	}
	
}
