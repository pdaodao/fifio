package com.github.apengda.fifio.elasticsearch;

import com.github.apengda.fifio.elasticsearch.catalog.MyJdbcCatalog;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
import com.github.apengda.fifio.elasticsearch.util.DbUtil;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.github.apengda.fifio.elasticsearch.catalog.MyJdbcCatalogFactoryOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

public class MyJdbcCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MyJdbcCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
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
        String baseUrl = helper.getOptions().get(BASE_URL);
        String defaultDbName = helper.getOptions().get(DEFAULT_DATABASE);

        DbInfo dbInfo = new DbInfo(
                DbUtil.buildUrl(baseUrl, defaultDbName),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD));

        return new MyJdbcCatalog(
                context.getName(),
                defaultDbName,
                dbInfo, null);
    }
}
