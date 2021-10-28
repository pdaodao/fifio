package com.github.apengda.fifio.odps.source;

import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class OdpsSourceEnumerator implements SplitEnumerator<OdpsPartitionSplit, OdpsSourceEnumState> {
    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<OdpsPartitionSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public OdpsSourceEnumState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
