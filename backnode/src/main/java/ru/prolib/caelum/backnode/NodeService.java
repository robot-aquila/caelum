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
import ru.prolib.caelum.backnode.mvc.ItemMvcAdapterIterator;
import ru.prolib.caelum.backnode.mvc.TupleMvcAdapterIterator;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.core.Tuple;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.ItemDataIteratorStub;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
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
	private final Periods periods;
	
	public NodeService(ICaelum caelum, Freemarker templates, JsonFactory json_factory, Periods periods) {
		this.caelum = caelum;
		this.templates = templates;
		this.jsonFactory = json_factory;
		this.periods = periods;
	}
	
	private Period validatePeriod(Period period) {
		if ( period == null ) {
			period = DEFAULT_PERIOD;
		}
		return period;
	}
	
	private Long validateFrom(Long from) {
		if ( from == null ) {
			from = 0L;
		} else if ( from < 0 ) {
			throw new BadRequestException("Time from expected to be >= 0 but: " + from);
		}
		return from;
	}
	
	private Long validateTo(Long to) {
		if ( to == null ) {
			to = System.currentTimeMillis();
		} else if ( to < 0 ) {
			throw new BadRequestException("Time to expected to be >= 0 but: " + to);
		}
		return to;
	}
	
	private void validateFromAndTo(Long from, Long to) {
		if ( to <= from ) {
			throw new BadRequestException("Time to expected to be > time from but: from=" + from + " to=" + to);
		}
	}
	
	private Long validateLimit(Long limit) {
		if ( limit == null ) {
			limit = DEFAULT_LIMIT;
		}
		if ( limit <= 0 ) {
			throw new BadRequestException("Limit expected to be > 0 but: " + limit);
		}
		if ( limit > MAX_LIMIT ) {
			throw new BadRequestException("Limit expected to be <= " + MAX_LIMIT + " but: " + limit);
		}
		return limit;
	}
	
	private Long validateOffset(Long offset) {
		if ( offset == null ) {
			offset = 0L;
		}
		if ( offset < 0 ) {
			throw new BadRequestException("Negative offset prohibited");
		}
		return offset;
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
	private AggregatedDataRequest toAggrDataRequest(String symbol, Period period, Long from, Long to, Long limit) {
		period = validatePeriod(period);
		from = validateFrom(from);
		to = validateTo(to);
		validateFromAndTo(from, to);
		limit = validateLimit(limit);
		return new AggregatedDataRequest(symbol, period, from, to, limit);
	}
	
	private ItemDataRequest toItemDataRequest(String symbol, Long from, Long to, Long limit) {
		from = validateFrom(from);
		to = validateTo(to);
		validateFromAndTo(from, to);
		limit = validateLimit(limit);
		return new ItemDataRequest(symbol, from, to, limit);
	}
	
	private ItemDataRequestContinue toItemDataRequestContinue(String symbol, Long offset,
			String magic, Long to, Long limit)
	{
		offset = validateOffset(offset);
		to = validateTo(to);
		limit = validateLimit(limit);
		return new ItemDataRequestContinue(symbol, offset, magic, to, limit);
	}
	
	@GET
	@Path("/ping")
	public Result<Void> ping() {
		return new Result<Void>(System.currentTimeMillis(), null);
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
		AggregatedDataRequest request = toAggrDataRequest(symbol, period, from, to, limit);
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
	@Path("/console/items")
	@Produces(MediaType.TEXT_HTML)
	public Response consoleItems(
			@QueryParam("symbol") final String symbol,
			@QueryParam("from") final String str_from,
			@QueryParam("to") final String str_to,
			@QueryParam("limit") final Long limit,
			@QueryParam("from_offset") final Long offset,
			@QueryParam("magic") final String magic)
	{
		Long from = null, to = null;
		if ( str_from != null && str_from.length() > 0 ) {
			from = Instant.parse(str_from).toEpochMilli();
		}
		if ( str_to != null && str_to.length() > 0 ) {
			to = Instant.parse(str_to).toEpochMilli();
		}
		boolean has_output = symbol != null && symbol.length() > 0;
		final Map<String, Object> model = new HashMap<>();
		IItemDataIterator item_iterator = null;
		boolean is_continue_request = false;
		Object request = null;
		if ( has_output ) {
			if ( offset == null ) {
				ItemDataRequest _request = toItemDataRequest(symbol, from, to, limit);
				item_iterator = caelum.fetch(_request);
				is_continue_request = false;
				request = _request;
			} else {
				ItemDataRequestContinue _request = toItemDataRequestContinue(symbol, offset, magic, to, limit); 
				item_iterator = caelum.fetch(_request);
				is_continue_request = true;
				request = _request;
			}
		} else {
			item_iterator = new ItemDataIteratorStub();
			ItemDataRequest _request = toItemDataRequest(symbol, from, to, limit);
			request = _request;
		}
		final ItemMvcAdapterIterator it = new ItemMvcAdapterIterator(item_iterator);
		model.put("is_continue_request", is_continue_request);
		model.put("request", request);
		if ( has_output ) {
			model.put("rows", it);
			model.put("magic", it.getMetaData().getMagic());
		}
		return Response.status(200)
			.entity((StreamingOutput)((stream) -> {
				try ( Writer out = new OutputStreamWriter(stream) ) {
					templates.getTemplate("/console_items.ftl").process(model, out);
				} catch ( TemplateException e ) {
					throw new IOException("Error processing template", e);
				} finally {
					try {
						it.close();
					} catch ( Exception e ) {
						logger.error("Unexpected exception: ", e);
					}
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
		if ( str_from != null && str_from.length() > 0 ) {
			from = Instant.parse(str_from).toEpochMilli();
		}
		if ( str_to != null && str_to.length() > 0 ) {
			to = Instant.parse(str_to).toEpochMilli();
		}
		AggregatedDataRequest request = toAggrDataRequest(symbol, period, from, to, limit);
		final boolean has_output = request.isValidSymbol();
		final WindowStoreIterator<Tuple> it = has_output ?
				new TupleMvcAdapterIterator(caelum.fetch(request)) : new WindowStoreIteratorStub<>();
		final Map<String, Object> model = new HashMap<>();
		model.put("request", request);
		model.put("periods", periods.getIntradayPeriodCodes());
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
