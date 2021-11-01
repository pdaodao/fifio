package com.github.apengda.fifio.odps.source;

import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsReadOptions;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;

public class OdpsDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown, SupportsLimitPushDown {
    private final OdpsOptions options;
    private final OdpsReadOptions readOptions;
    private TableSchema physicalSchema;

    public OdpsDynamicTableSource(
            OdpsOptions options,
            OdpsReadOptions readOptions,
            TableSchema physicalSchema) {
        this.options = options;
        this.readOptions = readOptions;
        this.physicalSchema = physicalSchema;
    }


    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final OdpsRowDataInputFormat.Builder builder = OdpsRowDataInputFormat.builder()
                .setOdpsOptions(options)
                .setReadOptions(readOptions)
                .setColumns(Arrays.asList(physicalSchema.getFieldNames()));
        builder.setRowDataTypeInfo(
                runtimeProviderContext.createTypeInformation(physicalSchema.toRowDataType()));

        return InputFormatProvider.of(builder.build());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // JDBC doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }

    @Override
    public DynamicTableSource copy() {
        return new OdpsDynamicTableSource(options, readOptions, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "odps:" + options.getProject() + "." + options.getTable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        OdpsDynamicTableSource that = (OdpsDynamicTableSource) o;

        return new EqualsBuilder()
                .append(options, that.options)
                .append(readOptions, that.readOptions)
                .append(physicalSchema, that.physicalSchema)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(options)
                .append(readOptions)
                .append(physicalSchema)
                .toHashCode();
    }

    @Override
    public void applyLimit(long limit) {
        this.readOptions.setLimit(limit);
    }
}
