package ru.prolib.caelum.restapi;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Result<T> {
	private final long time;
	private final int code;
	private final String msg;
	private final T data;
	
	public Result(long time, int code, String msg, T data) {
		this.time = time;
		this.code = code;
		this.msg = msg;
		this.data = data;
	}
	
	/**
	 * Constructor of successful result.
	 * <p>
	 * @param time - timestamp
	 * @param data - result data
	 */
	public Result(long time, T data) {
		this(time, ResultCode.OK, null, data);
	}
	
	/**
	 * Constructor of failed result.
	 * <p>
	 * @param time - time
	 * @param code - error code
	 * @param msg - error message
	 */
	public Result(long time, int code, String msg) {
		this(time, code, msg, null);
	}
	
	public long getTime() {
		return time;
	}
	
	public int getCode() {
		return code;
	}
	
	public String getMessage() {
		return msg;
	}
	
	public T getData() {
		return data;
	}
	
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(569127345, 7119)
				.append(time)
				.append(code)
				.append(msg)
				.append(data)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Result.class ) {
			return false;
		}
		Result<?> o = (Result<?>) other;
		return new EqualsBuilder()
				.append(o.time, time)
				.append(o.code, code)
				.append(o.msg, msg)
				.append(o.data, data)
				.build();
	}
	
	public boolean isError() {
		return code != ResultCode.OK;
	}
	
}
