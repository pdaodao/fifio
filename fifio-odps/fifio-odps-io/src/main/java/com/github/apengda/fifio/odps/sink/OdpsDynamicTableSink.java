package com.github.apengda.fifio.odps.sink;

import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsWriteOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;

public class OdpsDynamicTableSink implements DynamicTableSink {
    private final OdpsOptions options;
    private final OdpsWriteOptions writeOptions;
    private final TableSchema tableSchema;

    public OdpsDynamicTableSink(OdpsOptions options, OdpsWriteOptions writeOptions, TableSchema tableSchema) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final OdpsOutputFormatBuilder builder = new OdpsOutputFormatBuilder();
        builder.setOptions(options);
        builder.setWriteOptions(writeOptions);
        builder.setFieldNames(tableSchema.getFieldNames());
        builder.setFieldDataTypes(tableSchema.getFieldDataTypes());

        return OutputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new OdpsDynamicTableSink(options, writeOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "odps:write->" + options.getTable();
    }
}
