package com.github.apengda.fifio.odps;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class OdpsCatalogFactoryOptions {
    public static final String IDENTIFIER = "odps";

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue();

    public static final ConfigOption<String> PROJECT =
            ConfigOptions.key("project").stringType().noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();


    private OdpsCatalogFactoryOptions() {
    }
}
