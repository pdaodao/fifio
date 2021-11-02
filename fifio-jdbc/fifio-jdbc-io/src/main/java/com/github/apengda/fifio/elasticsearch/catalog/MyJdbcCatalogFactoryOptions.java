package com.github.apengda.fifio.elasticsearch.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class MyJdbcCatalogFactoryOptions {
    public static final String IDENTIFIER = "myjdbc";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> BASE_URL =
            ConfigOptions.key("base-url").stringType().noDefaultValue();

    private MyJdbcCatalogFactoryOptions() {
    }
}
