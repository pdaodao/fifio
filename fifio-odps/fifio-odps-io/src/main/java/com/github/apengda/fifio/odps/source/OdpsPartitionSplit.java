package com.github.apengda.fifio.odps.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class OdpsPartitionSplit implements SourceSplit {

    private final String sessionId;

    // 分片
    private final String partition;
    // 起始行
    private final Long start;
    // 行数
    private final Long step;

    public OdpsPartitionSplit(String sessionId, String partition, Long start, Long step) {
        this.sessionId = sessionId;
        this.partition = partition;
        this.start = start;
        this.step = step;
    }

    @Override
    public String splitId() {
        return null;
    }
}
