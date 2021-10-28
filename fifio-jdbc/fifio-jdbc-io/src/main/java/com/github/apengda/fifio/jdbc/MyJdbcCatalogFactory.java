package com.github.apengda.fifio.jdbc;

import com.github.apengda.fifio.jdbc.catalog.MyJdbcCatalog;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

public class MyJdbcCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MyJdbcCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return "myjdbc";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        return new MyJdbcCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(BASE_URL));
    }
}
