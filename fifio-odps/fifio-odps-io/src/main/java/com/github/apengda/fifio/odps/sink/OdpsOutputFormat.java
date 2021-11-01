package com.github.apengda.fifio.odps.sink;


import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.github.apengda.fifio.base.util.DateTimeUtil;
import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsWriteOptions;
import com.github.apengda.fifio.odps.util.OdpsUtil;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class OdpsOutputFormat extends RichOutputFormat<RowData> {
    private final OdpsOptions options;
    private final OdpsWriteOptions writeOptions;
    private final String[] fieldNames;
    private final DataType[] fieldDataTypes;

    private transient Odps odps;
    private transient TableTunnel.UploadSession uploadSession;
    private transient RecordWriter recordWriter;
    private transient List<Column> columns;
    private transient RowData.FieldGetter[] fieldGetters;

    public OdpsOutputFormat(OdpsOptions options, OdpsWriteOptions writeOptions, String[] fieldNames, DataType[] fieldDataTypes) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.fieldNames = fieldNames;
        this.fieldDataTypes = fieldDataTypes;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        odps = OdpsUtil.initOdps(options.toDbInfo());
        TableTunnel tunnel = new TableTunnel(odps);
        PartitionSpec partitionSpec = null;
        if (options.getPartition() != null) {
            partitionSpec = new PartitionSpec(options.getPartition());
        }
        try {
            if (partitionSpec == null) {
                uploadSession = tunnel.createUploadSession(options.getProject(),
                        options.getTable());
            } else {
                uploadSession = tunnel.createUploadSession(options.getProject(),
                        options.getTable(), partitionSpec);
            }
            columns = OdpsUtil.columnsInfo(uploadSession.getSchema(), Arrays.asList(fieldNames));
            recordWriter = uploadSession.openRecordWriter(taskNumber);
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }

        fieldGetters = new RowData.FieldGetter[fieldNames.length];
        int i = 0;
        for (DataType dataType : fieldDataTypes) {
            fieldGetters[i] = RowData.createFieldGetter(dataType.getLogicalType(), i++);
        }
    }

    @Override
    public void writeRecord(RowData record) throws IOException {
        if (record == null) {
            return;
        }
        Record row = uploadSession.newRecord();
        int index = 0;
        for (final Column field : columns) {
            Object v = fieldGetters[index].getFieldOrNull(record);
            if (v == null) {
                continue;
            }
            if (v instanceof BinaryStringData) {
                v = v.toString();
            }

            DataType columnType = fieldDataTypes[index];
            // doing
            switch (field.getTypeInfo().getOdpsType()) {
                case BIGINT: {
                    row.setBigint(field.getName(), Long.parseLong(v.toString()));
                    break;
                }
                case BOOLEAN: {
                    row.setBoolean(field.getName(), Boolean.valueOf(v.toString()));
                    break;
                }
                case DATETIME: {
                    row.setDatetime(field.getName(), DateTimeUtil.asDateTime(v));
                    break;
                }
                case DOUBLE: {
                    row.setDouble(field.getName(), Double.parseDouble(v.toString()));
                    break;
                }
                case STRING: {
                    row.setString(field.getName(), v.toString());
                    break;
                }
                default: {
                    throw new IOException("odps unknown field type " + field.getName() + ":" + field.getTypeInfo().getTypeName());
                }
            }
            index++;
        }

        recordWriter.write(row);
    }

    @Override
    public void close() throws IOException {
        if (recordWriter != null) {
            recordWriter.close();
            recordWriter = null;
            try {
                uploadSession.commit();
            } catch (TunnelException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }
}
