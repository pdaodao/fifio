package com.github.apengda.fifio.odps;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class OdpsConstant {

    public static final String IDENTIFIER = "odps";

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("url.");

    public static final ConfigOption<String> PROJECT = ConfigOptions
            .key("project")
            .stringType()
            .noDefaultValue()
            .withDescription("project");

    public static final ConfigOption<String> TABLE = ConfigOptions
            .key("table")
            .stringType()
            .noDefaultValue()
            .withDescription("table.");

    public static final ConfigOption<String> PARTITION = ConfigOptions
            .key("partition")
            .stringType()
            .noDefaultValue()
            .withDescription("partition.");

    public static final ConfigOption<Long> LIFECYCLE = ConfigOptions
            .key("life-cycle")
            .longType()
            .defaultValue(-1l)
            .withDescription("lifeCycle.");


    public static final ConfigOption<String> ACCESSID = ConfigOptions
            .key("access-id")
            .stringType()
            .noDefaultValue()
            .withDescription("the odps account accessId.");

    public static final ConfigOption<String> ACCESSKEY = ConfigOptions
            .key("access-key")
            .stringType()
            .noDefaultValue()
            .withDescription("the odps account accessKey.");

    // read data from odps
    public static final ConfigOption<Integer> READ_SPLIT_STEP_SIZE = ConfigOptions
            .key("read.splitStepSize")
            .intType()
            .noDefaultValue()
            .withDescription("readSplitStepSize:odps read split step size.");

    public static final ConfigOption<String> QUERY_TEMPLATE = ConfigOptions
            .key("read.sql")
            .stringType()
            .noDefaultValue()
            .withDescription("queryTemplate.");

    // read limit  from odps
    public static final ConfigOption<Long> READ_LIMIT = ConfigOptions
            .key("read.limit")
            .longType()
            .noDefaultValue()
            .withDescription("odps read limit for debug.");
}
