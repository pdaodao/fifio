package com.github.apengda.fifio.odps;

import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsReadOptions;
import com.github.apengda.fifio.odps.option.OdpsWriteOptions;
import com.github.apengda.fifio.odps.sink.OdpsDynamicTableSink;
import com.github.apengda.fifio.odps.source.OdpsDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static com.github.apengda.fifio.odps.OdpsConstant.*;

public class OdpsDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        helper.validate();
        validateConfigOptions(config);

        OdpsOptions options = getOptions(config);
        OdpsWriteOptions writeOptions = getWriteOptions(config);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new OdpsDynamicTableSink(
                options,
                writeOptions,
                physicalSchema);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

        return new OdpsDynamicTableSource(
                getOptions(config),
                getReadOptions(config),
                physicalSchema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(PROJECT);
        requiredOptions.add(TABLE);
        requiredOptions.add(ACCESSID);
        requiredOptions.add(ACCESSKEY);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(PARTITION);
        optionalOptions.add(QUERY_TEMPLATE);
        optionalOptions.add(READ_SPLIT_STEP_SIZE);
        optionalOptions.add(READ_LIMIT);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {

    }

    private OdpsOptions getOptions(ReadableConfig readableConfig) {
        final OdpsOptions.OdpsOptionsBuilder builder = OdpsOptions.builder()
                .setOdpsUrl(readableConfig.get(URL))
                .setProject(readableConfig.get(PROJECT))
                .setTable(readableConfig.get(TABLE))
                .setAccessId(readableConfig.get(ACCESSID))
                .setAccessKey(readableConfig.get(ACCESSKEY));
        readableConfig.getOptional(PARTITION).ifPresent(builder::setPartition);
        return builder.build();
    }

    private OdpsReadOptions getReadOptions(ReadableConfig readableConfig) {
        OdpsReadOptions readOptions = new OdpsReadOptions();
        readableConfig.getOptional(READ_SPLIT_STEP_SIZE).ifPresent(readOptions::setReadSplitStepSize);
        readableConfig.getOptional(READ_LIMIT).ifPresent(readOptions::setLimit);
        readableConfig.getOptional(QUERY_TEMPLATE).ifPresent(readOptions::setQueryTemplate);
        return readOptions;
    }

    private OdpsWriteOptions getWriteOptions(ReadableConfig readableConfig) {
        OdpsWriteOptions writeOptions = new OdpsWriteOptions();
        return writeOptions;
    }
}
