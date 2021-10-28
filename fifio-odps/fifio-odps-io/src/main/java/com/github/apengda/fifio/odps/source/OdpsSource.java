package com.github.apengda.fifio.odps.source;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class OdpsSource<OUT> implements Source<OUT, OdpsPartitionSplit, OdpsSourceEnumState> ,
        ResultTypeQueryable<OUT> {

    private final Boundedness boundedness;

    public OdpsSource(Boundedness boundedness) {
        this.boundedness = boundedness;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, OdpsPartitionSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<OdpsPartitionSplit, OdpsSourceEnumState> createEnumerator(SplitEnumeratorContext<OdpsPartitionSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<OdpsPartitionSplit, OdpsSourceEnumState> restoreEnumerator(SplitEnumeratorContext<OdpsPartitionSplit> enumContext, OdpsSourceEnumState checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<OdpsPartitionSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<OdpsSourceEnumState> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return null;
    }
}
