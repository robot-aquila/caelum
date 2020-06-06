package ru.prolib.caelum.restapi.node;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

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

import ru.prolib.caelum.aggregator.IKafkaStreamsRegistry;
import ru.prolib.caelum.aggregator.TupleAggregateIterator;
import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.restapi.TupleStreamerJson;
import ru.prolib.caelum.restapi.Result;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class NodeService implements IKafkaStreamsRegistry {
	static final Logger logger = LoggerFactory.getLogger(NodeService.class);
	public static final long MAX_LIMIT = 5000L;
	
	static class StoreDesc {
		final Period period;
		final String storeName;
		final KafkaStreams streams;
		
		public StoreDesc(Period period, String store_name, KafkaStreams streams) {
			this.period = period;
			this.storeName = store_name;
			this.streams = streams;
		}
		
	}
	
	private final ConcurrentHashMap<Period, StoreDesc> periodToStoreDesc = new ConcurrentHashMap<>();
	private final HostInfo hostInfo;
	private final JsonFactory jsonFactory = new JsonFactory();
	
	public NodeService(HostInfo host_info) {
		this.hostInfo = host_info;
	}

	@Override
	public void registerAggregator(Period period, String store_name, KafkaStreams streams) {
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
	
	@GET
	@Path("/ping")
	public Result<Void> ping() {
		return new Result<Void>(System.currentTimeMillis(), null);
	}
	
	@GET
	@Path("/tuples/{period}/{symbol}")
	public Response tuples(
			@NotNull
			@PathParam("period") final Period period,
			
			@NotNull
			@PathParam("symbol") final String symbol,
			
			@QueryParam("from") Long from,
			
			@QueryParam("to") Long to,
			
			@DefaultValue("500")
			@QueryParam("limit") long limit)
	{
		if ( from == null ) {
			from = 0L;
		} else if ( from < 0 ) {
			throw new BadRequestException("Time from expected to be >= 0 but: " + from);
		}
		if ( to == null ) {
			to = Long.MAX_VALUE;
		} else if ( to < 0 ) {
			throw new BadRequestException("Time to expected to be >= 0 but: " + to);
		}
		if ( to <= from ) {
			throw new BadRequestException("Time to expected to be > time from but: from=" + from + " to=" + to);
		}
		if ( limit <= 0 ) {
			throw new BadRequestException("Limit expected to be > 0 but: " + limit);
		}
		if ( limit > MAX_LIMIT ) {
			throw new BadRequestException("Limit expected to be <= " + MAX_LIMIT + " but: " + limit);
		}
		final AggregatedDataRequest request = new AggregatedDataRequest(symbol, period, from, to, limit);
		ReadOnlyWindowStore<String, Tuple> store = null;
		StreamingOutput streaming_output = null;
		StoreDesc desc = periodToStoreDesc.get(period);
		if ( desc == null ) {
			desc = periodToStoreDesc.get(Period.M1);
			if ( desc == null ) {
				throw new NotFoundException();
			}
			store = desc.streams.store(StoreQueryParameters.fromNameAndType(desc.storeName,
					QueryableStoreTypes.windowStore()));
			if ( store == null ) {
				throw new NotFoundException();
			}
			// TODO: Надо пропустить неполный период. Иначе в выборку попадет неполная свечка.
			streaming_output = new TupleStreamerJson(jsonFactory,
				new TupleAggregateIterator(store.fetch(symbol, request.getTimeFrom(), request.getTimeTo()),
						Periods.getInstance().getIntradayDuration(period)),
				request);
		} else {
			store = desc.streams.store(StoreQueryParameters.fromNameAndType(desc.storeName,
					QueryableStoreTypes.windowStore()));
			if ( store == null ) {
				throw new NotFoundException();
			}
			streaming_output = new TupleStreamerJson(jsonFactory,
					store.fetch(symbol, request.getTimeFrom(), request.getTimeTo()),
					request);
		}
		return Response.status(200)
				.entity(streaming_output)
				.build();
	}
	
	@GET
	@Path("/tuples/processors/{period}")
	public List<ProcessorMetadata> processors(@PathParam("period") final String period) {
		StoreDesc desc = periodToStoreDesc.get(period);
		if ( desc == null ) {
			throw new NotFoundException();
		}
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
	
	@GET
	@Path("/test/error")
	public void error() throws Exception {
		throw new Exception("Test error");
	}
}
