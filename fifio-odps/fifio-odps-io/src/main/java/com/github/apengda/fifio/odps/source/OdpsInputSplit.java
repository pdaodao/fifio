package com.github.apengda.fifio.odps.source;

import com.google.common.base.Objects;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;

@Public
public class OdpsInputSplit implements InputSplit, java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final String sessionId;
    // 编号
    private final int splitNum;
    // 分片
    private final String partition;
    // 起始行
    private final Long start;
    // 行数
    private final Long step;
    // 查询语句 如果不为空则 使用sql 查询
    private final String queryTemplate;

    public OdpsInputSplit(String sessionId, int splitNum, String partition, Long start, Long step, String queryTemplate) {
        this.sessionId = sessionId;
        this.splitNum = splitNum;
        this.partition = partition;
        this.start = start;
        this.step = step;
        this.queryTemplate = queryTemplate;
    }

    public String getSessionId() {
        return sessionId;
    }

    @Override
    public int getSplitNumber() {
        return this.splitNum;
    }

    public String getPartition() {
        return partition;
    }

    public Long getStart() {
        return start;
    }

    public Long getStep() {
        return step;
    }

    public String getQueryTemplate() {
        return queryTemplate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        OdpsInputSplit split = (OdpsInputSplit) o;

        return new EqualsBuilder()
                .append(splitNum, split.splitNum)
                .append(sessionId, split.sessionId)
                .append(partition, split.partition)
                .append(start, split.start)
                .append(step, split.step)
                .append(queryTemplate, split.queryTemplate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(sessionId)
                .append(splitNum)
                .append(partition)
                .append(start)
                .append(step)
                .toHashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("sessionId", sessionId)
                .add("splitNum", splitNum)
                .add("partition", partition)
                .add("start", start)
                .add("step", step)
                .add("queryTemplate", queryTemplate)
                .toString();
    }
}