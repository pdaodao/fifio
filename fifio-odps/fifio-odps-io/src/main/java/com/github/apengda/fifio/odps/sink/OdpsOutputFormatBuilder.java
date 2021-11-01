package com.github.apengda.fifio.odps.sink;

import com.github.apengda.fifio.odps.option.OdpsOptions;
import com.github.apengda.fifio.odps.option.OdpsWriteOptions;
import org.apache.flink.table.types.DataType;

public class OdpsOutputFormatBuilder {
    private OdpsOptions options;
    private OdpsWriteOptions writeOptions;
    private String[] fieldNames;
    private DataType[] fieldDataTypes;

    public OdpsOutputFormatBuilder setOptions(OdpsOptions options) {
        this.options = options;
        return this;
    }

    public OdpsOutputFormatBuilder setWriteOptions(OdpsWriteOptions writeOptions) {
        this.writeOptions = writeOptions;
        return this;
    }

    public OdpsOutputFormatBuilder setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public OdpsOutputFormatBuilder setFieldDataTypes(DataType[] fieldDataTypes) {
        this.fieldDataTypes = fieldDataTypes;
        return this;
    }

    public OdpsOutputFormat build() {
        return new OdpsOutputFormat(options, writeOptions, fieldNames, fieldDataTypes);
    }
}
