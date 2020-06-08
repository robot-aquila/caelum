package ru.prolib.caelum.backnode;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

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
import javax.ws.rs.core.StreamingOutput;

import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;

import freemarker.template.TemplateException;
import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.WindowStoreIteratorStub;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.backnode.mvc.TupleMvcAdapterIterator;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.service.ICaelum;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class NodeService {
	static final Logger logger = LoggerFactory.getLogger(NodeService.class);
	public static final long MAX_LIMIT = 5000L;
	public static final long DEFAULT_LIMIT = 500L;
	public static final Period DEFAULT_PERIOD = Period.M5;
	
	private final ICaelum caelum;
	private final Freemarker templates;
	private final JsonFactory jsonFactory;
	
	public NodeService(ICaelum caelum, Freemarker templates, JsonFactory json_factory) {
		this.caelum = caelum;
		this.templates = templates;
		this.jsonFactory = json_factory;
	}
	
	@GET
	@Path("/ping")
	public Result<Void> ping() {
		return new Result<Void>(System.currentTimeMillis(), null);
	}
	
	/**
	 * Validate data and build request or throw an exception in case of error.
	 * <p>
	 * @param symbol - symbol
	 * @param period - period
	 * @param from - from
	 * @param to - to
	 * @param limit - limit
	 * @return request
	 * @throws BadRequestException - invalid data
	 */
	private AggregatedDataRequest toRequest(String symbol, Period period, Long from, Long to, Long limit) {
		if ( period == null ) {
			period = DEFAULT_PERIOD;
		}
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
		if ( limit == null ) {
			limit = DEFAULT_LIMIT;
		}
		if ( limit <= 0 ) {
			throw new BadRequestException("Limit expected to be > 0 but: " + limit);
		}
		if ( limit > MAX_LIMIT ) {
			throw new BadRequestException("Limit expected to be <= " + MAX_LIMIT + " but: " + limit);
		}
		return new AggregatedDataRequest(symbol, period, from, to, limit);
	}
	
	@GET
	@Path("/tuples/{period}/{symbol}")
	public Response tuples(
			@PathParam("period") @NotNull final Period period,
			@PathParam("symbol") @NotNull final String symbol,
			@QueryParam("from") final Long from,
			@QueryParam("to") final Long to,
			@QueryParam("limit") @DefaultValue("500") final long limit)
	{
		AggregatedDataRequest request = toRequest(symbol, period, from, to, limit);
		return Response.status(200)
			.entity(new TupleStreamerJson(jsonFactory, caelum.fetch(request), request))
			.build();
	}
	
	@GET
	@Path("/console")
	@Produces(MediaType.TEXT_HTML)
	public Response consoleIndex() {
		return Response.status(200)
			.entity((StreamingOutput)((stream) -> {
				try ( Writer out = new OutputStreamWriter(stream) ) {
					templates.getTemplate("/console_index.ftl").process(new Object(), out);
				} catch ( TemplateException e ) {
					throw new IOException("Error processing template", e);
				}
			})).build();
	}
	
	@GET
	@Path("/console/tuples")
	@Produces(MediaType.TEXT_HTML)
	public Response consoleTuples(
			@QueryParam("symbol") final String symbol,
			@QueryParam("period") final Period period,
			@QueryParam("from") final String str_from,
			@QueryParam("to") final String str_to,
			@QueryParam("limit") final Long limit)
	{
		Long from = null, to = null;
		if ( str_from != null ) {
			from = Instant.parse(str_from).toEpochMilli();
		}
		if ( str_to != null ) {
			to = Instant.parse(str_to).toEpochMilli();
		}
		AggregatedDataRequest request = toRequest(symbol, period, from, to, limit);
		final boolean has_output = request.isValidSymbol();
		final WindowStoreIterator<Tuple> it = has_output ?
				new TupleMvcAdapterIterator(caelum.fetch(request)) : new WindowStoreIteratorStub<>();
		final Map<String, Object> model = new HashMap<>();
		model.put("request", request);
		if ( has_output ) {
			model.put("rows", it);
		}
		return Response.status(200)
			.entity((StreamingOutput)((stream) -> {
				try ( Writer out = new OutputStreamWriter(stream) ) {
					templates.getTemplate("/console_tuples.ftl").process(model, out);
				} catch ( TemplateException e ) {
					throw new IOException("Error processing template", e);
				} finally {
					it.close();
				}
			})).build();
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
