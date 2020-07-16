package ru.prolib.caelum.backnode;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.TemplateException;
import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.backnode.mvc.ItemMvcAdapterIterator;
import ru.prolib.caelum.backnode.mvc.StreamFactory;
import ru.prolib.caelum.backnode.mvc.TupleMvcAdapter;
import ru.prolib.caelum.backnode.mvc.TupleMvcAdapterIterator;
import ru.prolib.caelum.core.ByteUtils;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemIteratorStub;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class NodeService {
	static final Logger logger = LoggerFactory.getLogger(NodeService.class);
	public static final long MAX_LIMIT = 5000L;
	public static final int DEFAULT_LIMIT = 1000;
	public static final Period DEFAULT_PERIOD = Period.M5;
	
	private final ICaelum caelum;
	private final Freemarker templates;
	private final StreamFactory streamFactory;
	private final Periods periods;
	private final ByteUtils byteUtils;
	
	public NodeService(ICaelum caelum,
			Freemarker templates,
			StreamFactory streamFactory,
			Periods periods,
			ByteUtils byteUtils)
	{
		this.caelum = caelum;
		this.templates = templates;
		this.streamFactory = streamFactory;
		this.periods = periods;
		this.byteUtils = byteUtils;
	}
	
	public ICaelum getCaelum() {
		return caelum;
	}
	
	public Freemarker getFreemarker() {
		return templates;
	}
	
	public StreamFactory getStreamFactory() {
		return streamFactory;
	}
	
	public Periods getPeriods() {
		return periods;
	}
	
	public ByteUtils getByteUtils() {
		return byteUtils;
	}
	
	private String validateSymbol(String symbol) {
		if ( symbol == null || symbol.length() == 0 ) {
			throw new BadRequestException("Symbol cannot be null");
		}
		return symbol;
	}
	
	private String validateCategory(String category) {
		if ( category == null || category.length() == 0 ) {
			throw new BadRequestException("Category cannot be null");
		}
		return category;
	}
	
	private Period validatePeriod(Period period) {
		if ( period == null ) {
			period = DEFAULT_PERIOD;
		}
		return period;
	}
	
	private Long validateTime(Long time) {
		if ( time == null || time < 0 ) {
			throw new BadRequestException("Time cannot be null or negative but: " + time);
		}
		return time;
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
	
	private Integer validateLimit(Integer limit) {
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
	private AggregatedDataRequest toAggrDataRequest(String symbol, Period period, Long from, Long to, Integer limit) {
		period = validatePeriod(period);
		from = validateFrom(from);
		to = validateTo(to);
		validateFromAndTo(from, to);
		limit = validateLimit(limit);
		return new AggregatedDataRequest(symbol, period, from, to, limit);
	}
	
	private ItemDataRequest toItemDataRequest(String symbol, Long from, Long to, Integer limit) {
		from = validateFrom(from);
		to = validateTo(to);
		validateFromAndTo(from, to);
		limit = validateLimit(limit);
		return new ItemDataRequest(symbol, from, to, limit);
	}
	
	private ItemDataRequestContinue toItemDataRequestContinue(String symbol, Long offset,
			String magic, Long to, Integer limit)
	{
		offset = validateOffset(offset);
		to = validateTo(to);
		limit = validateLimit(limit);
		return new ItemDataRequestContinue(symbol, offset, magic, to, limit);
	}
	
	private SymbolListRequest toSymbolListRequest(String category, String afterSymbol, Integer limit) {
		return new SymbolListRequest(validateCategory(category), afterSymbol, validateLimit(limit));
	}
	
	private Result<Void> success() {
		return new Result<Void>(System.currentTimeMillis(), null);
	}
	
	private long toLong(BigDecimal value) {
		try {
			return byteUtils.centsToLong(value);
		} catch ( ArithmeticException e ) {
			throw new BadRequestException("Unsupported decimal size: " + value.toPlainString(), e);
		}
	}
	
	private byte toNumberOfDecimals(BigDecimal value) {
		if ( byteUtils.isNumberOfDecimalsFits4Bits(value.scale()) == false ) {
			throw new BadRequestException("Unsupported number of decimals: " + value.toPlainString());
		}
		return (byte) value.scale();
	}
	
	@GET
	@Path("/ping")
	public Result<Void> ping() {
		return success();
	}
	
	@GET
	@Path("/tuples/{period}")
	public Response tuples(
			@PathParam("period") @NotNull final Period period,
			@QueryParam("symbol") @NotNull final String symbol,
			@QueryParam("from") final Long from,
			@QueryParam("to") final Long to,
			@QueryParam("limit") final Integer limit)
	{
		AggregatedDataRequest request = toAggrDataRequest(symbol, period, from, to, limit);
		return Response.status(200)
			.entity(streamFactory.tuplesToJson(caelum.fetch(request), request))
			.build();
	}
	
	@GET
	@Path("/items")
	public Response items(
			@QueryParam("symbol") @NotNull final String symbol,
			@QueryParam("from") final Long from,
			@QueryParam("from_offset") final Long from_offset,
			@QueryParam("to") final Long to,
			@QueryParam("limit") final Integer limit,
			@QueryParam("magic") final String magic)
	{
		if ( from_offset == null ) {
			ItemDataRequest request = toItemDataRequest(symbol, from, to, limit);
			return Response.status(200)
				.entity(streamFactory.itemsToJson(caelum.fetch(request), request))
				.build();
		} else {
			ItemDataRequestContinue request = toItemDataRequestContinue(symbol, from_offset, magic, to, limit);
			return Response.status(200)
				.entity(streamFactory.itemsToJson(caelum.fetch(request), request))
				.build();
		}
	}
	
	@PUT
	@Path("/item")
	public Result<Void> item(
			@QueryParam("symbol") @NotNull final String symbol,
			@QueryParam("time") @NotNull final Long time,
			@QueryParam("value") @NotNull final BigDecimal bd_value,
			@QueryParam("volume") @NotNull final BigDecimal bd_volume)
	{
		caelum.registerItem(Item.ofDecimax15(validateSymbol(symbol), validateTime(time),
				toLong(bd_value), toNumberOfDecimals(bd_value),
				toLong(bd_volume), toNumberOfDecimals(bd_volume)));
		return success();
	}
	
	@GET
	@Path("/categories")
	public Response categories() {
		return Response.status(200)
			.entity(streamFactory.categoriesToJson(caelum.fetchCategories()))
			.build();
	}
	
	@GET
	@Path("/symbols")
	public Response symbols(
			@QueryParam("category") @NotNull final String category,
			@QueryParam("after_symbol") final String after_symbol,
			@QueryParam("limit") final Integer limit)
	{
		SymbolListRequest request = toSymbolListRequest(category, after_symbol, limit);
		return Response.status(200)
			.entity(streamFactory.symbolsToJson(caelum.fetchSymbols(request), request))
			.build();
	}
	
	@GET
	@Path("/symbol/updates")
	public Response symbolUpdates(@QueryParam("symbol") @NotNull final String symbol) {
		return Response.status(200)
			.entity(streamFactory.symbolUpdatesToJson(caelum.fetchSymbolUpdates(validateSymbol(symbol)), symbol))
			.build();
	}
	
	@PUT
	@Path("/symbol/update")
	public Result<Void> symbolUpdate(@Context HttpServletRequest request) {
		String symbol = null;
		Long time = null;
		Map<Integer, String> tokens = new LinkedHashMap<>();
		Enumeration<String> param_list = request.getParameterNames();
		while ( param_list.hasMoreElements() ) {
			String param = param_list.nextElement(), value = request.getParameter(param);
			if ( "symbol".equals(param) ) {
				symbol = request.getParameter(param);
			} else if ( "time".equals(param) ) {
				time = Long.parseLong(request.getParameter(param));
			} else if ( StringUtils.isNumeric(param) ) {
				try {
					tokens.put(Integer.parseInt(param), value);
				} catch ( NumberFormatException e ) {
					throw new BadRequestException("Illegal token: " + param);
				}
			} else {
				throw new BadRequestException("Unknown parameter or parameter type: " + param);
			}
		}
		caelum.registerSymbolUpdate(new SymbolUpdate(validateSymbol(symbol), validateTime(time), tokens));
		return success(); 
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
			@QueryParam("limit") final Integer limit,
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
		IItemIterator item_iterator = null;
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
			item_iterator = new ItemIteratorStub();
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
			@QueryParam("limit") final Integer limit)
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
		final ICloseableIterator<TupleMvcAdapter> it = has_output ?
				new TupleMvcAdapterIterator(caelum.fetch(request)) : new IteratorStub<>();
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
					try {
						it.close();
					} catch ( Exception e ) {
						throw new IOException(e);
					}
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

}
