package com.github.apengda.fifio.odps.source;

import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsReadOptions;
import com.github.apengda.fifio.odps.util.OdpsFlinkUtil;
import com.github.apengda.fifio.odps.util.OdpsUtil;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OdpsRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OdpsRowDataInputFormat.class);

    final private OdpsOptions odpsOptions;
    final private OdpsReadOptions readOptions;
    final private List<String> columns;
    final private TypeInformation<RowData> rowDataTypeInfo;

    private transient Odps odps;
    private transient TunnelRecordReader recordReader;

    private transient List<Column> selectColumns;
    private transient com.aliyun.odps.data.Record nextRecord;

    private OdpsRowDataInputFormat(
            OdpsOptions odpsOptions,
            OdpsReadOptions readOptions,
            List<String> columns,
            TypeInformation<RowData> rowDataTypeInfo) {
        this.odpsOptions = odpsOptions;
        this.readOptions = readOptions;
        this.columns = columns;
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void openInputFormat() {
        odps = OdpsUtil.initOdps(odpsOptions.toDbInfo());
    }


    @Override
    public void closeInputFormat() {

    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        OdpsInputSplit split = (OdpsInputSplit) inputSplit;
        try {
            if (split.getQueryTemplate() == null) {
                // tunnel 下载
                TableTunnel tableTunnel = new TableTunnel(odps);
                TableTunnel.DownloadSession downloadSession =
                        OdpsFlinkUtil.createDownloadSession(tableTunnel, odpsOptions, split.getPartition(), split.getSessionId());
                selectColumns = OdpsUtil.columnsInfo(downloadSession.getSchema(), columns);
                recordReader = downloadSession.openRecordReader(split.getStart(), split.getStep(), false, selectColumns);
            } else {
                // sql 查询
                String sql = split.getQueryTemplate().trim();
                if (!sql.endsWith(";")) {
                    sql = sql + ";";
                }

                Instance i = SQLTask.run(odps, sql);
                i.waitForSuccess();
                //创建InstanceTunnel。
                InstanceTunnel tunnel = new InstanceTunnel(odps);
                InstanceTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(), i.getId());
                selectColumns = OdpsUtil.columnsInfo(session.getSchema(), columns);
                long count = session.getRecordCount();
                //获取数据的写法与TableTunnel一样。
                recordReader = session.openRecordReader(0, count);
            }
        } catch (OdpsException e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    @Override
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return rowDataTypeInfo;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        nextRecord = recordReader.read();
        return nextRecord == null;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        try {
            // 处理数据转换
            RowData row = rowConvert(nextRecord);
            return row;
        } catch (Exception se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        }
    }

    /**
     * 数据转换
     *
     * @param record
     * @return
     */
    private RowData rowConvert(Record record) {
        if (record == null) {
            return null;
        }
        GenericRowData row = new GenericRowData(selectColumns.size());
        int index = 0;
        for (Column f : selectColumns) {
            OdpsType type = f.getTypeInfo().getOdpsType();
            Object value = null;

            switch (type) {
                case BIGINT:
                    value = record.getBigint(index);
                    break;
                case BOOLEAN:
                    value = record.getBoolean(index);
                    break;
                case DATETIME:
                    value = record.getDatetime(index);
                    break;
                case STRING: {
                    value = record.getString(index);
                    if (value != null) {
                        value = BinaryStringData.fromString((String) value);
//                        value = StringData.fromString((String) value);
                    }
                    break;
                }
                case DOUBLE:
                    value = record.getDouble(index);
                    break;
                default:
                    value = record.get(index);
            }

            row.setField(index++, value);
        }
        return row;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    // 切分
    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        final Odps odps = OdpsUtil.initOdps(odpsOptions.toDbInfo());
        InputSplit[] sp = OdpsFlinkUtil.split(odps, odpsOptions, readOptions);
        return sp;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    /**
     * Builder for {@link OdpsRowDataInputFormat}.
     */
    public static class Builder {
        private OdpsOptions odpsOptions;
        private OdpsReadOptions readOptions;
        private List<String> columns;
        private TypeInformation<RowData> rowDataTypeInfo;

        public Builder setOdpsOptions(OdpsOptions odpsOptions) {
            this.odpsOptions = odpsOptions;
            return this;
        }

        public Builder setReadOptions(OdpsReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public Builder setColumns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder setRowDataTypeInfo(TypeInformation<RowData> rowDataTypeInfo) {
            this.rowDataTypeInfo = rowDataTypeInfo;
            return this;
        }

        public OdpsRowDataInputFormat build() {
            if (this.odpsOptions == null) {
                throw new IllegalArgumentException("No odpsOptions supplied");
            }
            return new OdpsRowDataInputFormat(
                    odpsOptions,
                    readOptions,
                    this.columns,
                    this.rowDataTypeInfo);
        }
    }
}
