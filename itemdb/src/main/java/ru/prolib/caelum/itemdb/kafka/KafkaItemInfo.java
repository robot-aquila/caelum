package ru.prolib.caelum.itemdb.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.common.TopicPartition;

public class KafkaItemInfo {
	private final String topic;
	private final String symbol;
	private final int numPartitions, partition;
	private final Long startOffset, endOffset;
	
	public KafkaItemInfo(String topic, int num_partitions, String symbol, int partition,
			Long start_offset, Long end_offset)
	{
		this.topic = topic;
		this.numPartitions = num_partitions;
		this.symbol = symbol;
		this.partition = partition;
		this.startOffset = start_offset;
		this.endOffset = end_offset;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public long getNumPartitions() {
		return numPartitions;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public int getPartition() {
		return partition;
	}
	
	public Long getStartOffset() {
		return startOffset;
	}
	
	public Long getEndOffset() {
		return endOffset;
	}
	
	public boolean hasData() {
		return startOffset != null && endOffset != null;
	}
	
	public TopicPartition toTopicPartition() {
		return new TopicPartition(topic, partition);
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("topic", topic)
				.append("numPartitions", numPartitions)
				.append("symbol", symbol)
				.append("partition", partition)
				.append("startOffset", startOffset)
				.append("endOffset", endOffset)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1403597, 11)
				.append(topic)
				.append(numPartitions)
				.append(symbol)
				.append(partition)
				.append(startOffset)
				.append(endOffset)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemInfo.class ) {
			return false;
		}
		KafkaItemInfo o = (KafkaItemInfo) other;
		return new EqualsBuilder()
				.append(o.topic, topic)
				.append(o.numPartitions, numPartitions)
				.append(o.symbol, symbol)
				.append(o.partition, partition)
				.append(o.startOffset, startOffset)
				.append(o.endOffset, endOffset)
				.build();
	}

}
