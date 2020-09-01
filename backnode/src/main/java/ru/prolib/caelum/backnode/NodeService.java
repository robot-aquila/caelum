package ru.prolib.caelum.backnode;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.aggregator.AggregatedDataResponse;
import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.backnode.mvc.StreamFactory;
import ru.prolib.caelum.core.ByteUtils;
import ru.prolib.caelum.core.HostInfo;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

@Path("/api/v1/")
@Produces(MediaType.APPLICATION_JSON)
public class NodeService {
	static final Logger logger = LoggerFactory.getLogger(NodeService.class);
	public static final Period DEFAULT_PERIOD = Period.M5;
	
	static class RowsWrapper<T> {
		public final List<T> rows;
		
		public RowsWrapper(List<T> rows) {
			this.rows = rows;
		}
	}
	
	private final ICaelum caelum;
	private final StreamFactory streamFactory;
	private final Periods periods;
	private final ByteUtils byteUtils;
	private final boolean testMode;
	
	public NodeService(ICaelum caelum,
			StreamFactory streamFactory,
			Periods periods,
			ByteUtils byteUtils,
			boolean testMode)
	{
		this.caelum = caelum;
		this.streamFactory = streamFactory;
		this.periods = periods;
		this.byteUtils = byteUtils;
		this.testMode = testMode;
	}
	
	public ICaelum getCaelum() {
		return caelum;
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
	
	public boolean isTestMode() {
		return testMode;
	}
	
	private String validateSymbol(String symbol) {
		if ( symbol == null || symbol.length() == 0 ) {
			throw new BadRequestException("Symbol cannot be null");
		}
		return symbol;
	}
	
	private String validateCategory(String category) {
		if ( category == null ) {
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
		if ( from != null && from < 0 ) {
			throw new BadRequestException("Time from expected to be >= 0 but: " + from);
		}
		return from;
	}
	
	private Long validateTo(Long to) {
		if ( to != null && to < 0 ) {
			throw new BadRequestException("Time to expected to be >= 0 but: " + to);
		}
		return to;
	}
	
	private void validateFromAndTo(Long from, Long to) {
		if ( from != null && to != null && to <= from ) {
			throw new BadRequestException("Time to expected to be > time from but: from=" + from + " to=" + to);
		}
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
		return new AggregatedDataRequest(symbol, period, from, to, limit);
	}
	
	private ItemDataRequest toItemDataRequest(String symbol, Long from, Long to, Integer limit) {
		return new ItemDataRequest(symbol, from, to, limit);
	}
	
	private ItemDataRequestContinue toItemDataRequestContinue(String symbol, Long offset,
			String magic, Long to, Integer limit)
	{
		return new ItemDataRequestContinue(symbol, offset, magic, to, limit);
	}
	
	private SymbolListRequest toSymbolListRequest(String category, String afterSymbol, Integer limit) {
		return new SymbolListRequest(validateCategory(category), afterSymbol, limit);
	}
	
	private Result<Void> success() {
		return new Result<Void>(System.currentTimeMillis(), null);
	}
	
	private <T> Result<T> success(T data) {
		return new Result<>(System.currentTimeMillis(), data);
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
	@Path("/clear")
	public Result<Void> clear(@QueryParam("global") final boolean global) {
		if ( ! isTestMode() ) {
			throw new ForbiddenException();
		}
		caelum.clear(global);
		return success();
	}
	
	@GET
	@Path("/logMarker")
	public Result<Void> logMarker(@QueryParam("marker") @NotNull final String marker) {
		if ( ! isTestMode() ) {
			throw new ForbiddenException();
		}
		logger.debug(marker);
		return success();
	}
	
	private URL replaceHostAndPort(UriInfo uriInfo, HostInfo hostInfo) throws MalformedURLException {
		URL myUrl = uriInfo.getRequestUri().toURL();
		URL toUrl = new URL(myUrl.getProtocol(), hostInfo.getHost(),
				hostInfo.getPort(), myUrl.getPath() + "?" + myUrl.getQuery());
		return toUrl;
	}
	
	@GET
	@Path("/tuples/{period}")
	public Response tuples(
			@Context UriInfo uriInfo,
			@PathParam("period") @NotNull final Period period,
			@QueryParam("symbol") @NotNull final String symbol,
			@QueryParam("from") final Long from,
			@QueryParam("to") final Long to,
			@QueryParam("limit") final Integer limit) throws IOException
	{
		AggregatedDataRequest request = toAggrDataRequest(symbol, period, from, to, limit);
		AggregatedDataResponse response = caelum.fetch(request);
		if ( response.askAnotherHost() ) {
			try {
				URL url = replaceHostAndPort(uriInfo, response.getHostInfo());
				final HttpURLConnection con = (HttpURLConnection) url.openConnection();
				con.setConnectTimeout(10000);
				con.setReadTimeout(10000);
				con.connect();
				final InputStream input = con.getInputStream();
				return Response.status(con.getResponseCode())
					.entity((StreamingOutput)(output) -> {
						IOUtils.copy(input, output);
						input.close();
					}).build();
			} catch ( Exception e ) {
				throw new IllegalStateException("Error querying another node: " + response.getHostInfo(), e);
			}
		}
		return Response.status(200)
			.entity(streamFactory.tuplesToJson(response.getResult(), request))
			.build();
	}
	
	@GET
	@Path("/periods")
	public Response periods() {
		List<String> rows = caelum.getAggregationPeriods()
				.stream()
				.map(x -> x.toString())
				.collect(Collectors.toList());
		return Response.status(200)
			.entity(streamFactory.stringsToJson(new IteratorStub<>(rows)))
			.build();
	}
	
	@GET
	@Path("/aggregator/status")
	public Result<RowsWrapper<AggregatorStatus>> aggregatorStatus() {
		return success(new RowsWrapper<>(caelum.getAggregatorStatus()));
	}
	
	@GET
	@Path("/items")
	public Response items(
			@QueryParam("symbol") @NotNull final String symbol,
			@QueryParam("from") final Long from,
			@QueryParam("fromOffset") final Long from_offset,
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
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Result<Void> item(
			@FormParam("symbol") List<String> raw_symbol,
			@FormParam("time") List<String> raw_time,
			@FormParam("value") List<String> raw_bd_value,
			@FormParam("volume") List<String> raw_bd_volume)
	{
		if ( ! isTestMode() ) {
			throw new ForbiddenException();
		}
		int count = raw_symbol.size();
		if ( raw_time.size() != count || raw_bd_value.size() != count || raw_bd_volume.size() != count ) {
			throw new BadRequestException();
		}
		List<IItem> items = new ArrayList<>(count);
		for ( int i = 0; i < count; i ++ ) {
			long time;
			BigDecimal bd_value, bd_volume;
			try {
				time = validateTime(Long.parseLong(raw_time.get(i)));
			} catch ( NumberFormatException e ) {
				throw new BadRequestException("Illegal time at position #" + i + ": " + raw_time.get(i));
			}
			try {
				bd_value = new BigDecimal(raw_bd_value.get(i));
			} catch ( NumberFormatException e ) {
				throw new BadRequestException("Illegal value at position #" + i + ": " + raw_bd_value.get(i));
			}
			try {
				bd_volume = new BigDecimal(raw_bd_volume.get(i));
			} catch ( NumberFormatException e ) {
				throw new BadRequestException("Illegal volume at position #" + i + ": " + raw_bd_volume.get(i));
			}
			items.add(Item.ofDecimax15(validateSymbol(raw_symbol.get(i)), validateTime(time),
					toLong(bd_value), toNumberOfDecimals(bd_value),
					toLong(bd_volume), toNumberOfDecimals(bd_volume)));
		}
		caelum.registerItem(items);
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
			@QueryParam("category") String category,
			@QueryParam("afterSymbol") final String after_symbol,
			@QueryParam("limit") final Integer limit)
	{
		if ( category == null ) category = "";
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
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Result<Void> symbolUpdate(MultivaluedMap<String, String> params) {
		String symbol = null;
		Long time = null;
		Map<Integer, String> tokens = new LinkedHashMap<>();
		for ( String param : params.keySet() ) {
			String value = params.getFirst(param);
			if ( "symbol".equals(param) ) {
				symbol = value;
			} else if ( "time".equals(param) ) {
				time = Long.parseLong(value);
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
	
	@PUT
	@Path("/symbol")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Result<Void> symbol(@FormParam("symbol") List<String> symbols) {
		for ( String symbol : symbols ) validateSymbol(symbol);
		caelum.registerSymbol(symbols);
		return success();
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
