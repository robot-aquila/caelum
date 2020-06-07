package ru.prolib.caelum.backnode;

import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.service.ICaelum;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class NodeService {
	static final Logger logger = LoggerFactory.getLogger(NodeService.class);
	public static final long MAX_LIMIT = 5000L;
	
	private final ICaelum caelum;
	private final JsonFactory jsonFactory = new JsonFactory();
	
	public NodeService(ICaelum caelum) {
		this.caelum = caelum;
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
			to = System.currentTimeMillis();
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
		
		AggregatedDataRequest request = new AggregatedDataRequest(symbol, period, from, to, limit);
		return Response.status(200)
				.entity(new TupleStreamerJson(jsonFactory, caelum.fetch(request), request))
				.build();
	}
	
//	@GET
//	@Path("/tuples/processors/{period}")
//	public List<ProcessorMetadata> processors(@PathParam("period") final String period) {
//		StoreDesc desc = periodToStoreDesc.get(period);
//		if ( desc == null ) {
//			throw new NotFoundException();
//		}
//		for ( StreamsMetadata md : desc.streams.allMetadataForStore(desc.storeName) ) {
//			logger.info("Metadata: host={} port={}", md.host(), md.port());
//		}
//		return desc.streams.allMetadataForStore(desc.storeName).stream()
//			.map(md -> new ProcessorMetadata(md.host(), md.port(), md.topicPartitions().stream()
//					.map(TopicPartition::partition)
//					.collect(Collectors.toList()))
//			)
//			.collect(Collectors.toList());
//	}
//	
//	@GET
//	@Path("/test/error")
//	public void error() throws Exception {
//		throw new Exception("Test error");
//	}

}
