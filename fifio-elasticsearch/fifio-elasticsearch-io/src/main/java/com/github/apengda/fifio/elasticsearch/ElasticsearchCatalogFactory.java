package com.github.apengda.fifio.elasticsearch;

import com.github.apengda.fifio.elasticsearch.catalog.ElasticsearchCatalog;
import com.github.apengda.fifio.elasticsearch.dialect.ElasticsearchDialect;
import com.github.apengda.fifio.elasticsearch.frame.DbInfo;
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

public class ElasticsearchCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return ElasticsearchDialect.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        String baseUrl = helper.getOptions().get(BASE_URL);
        DbInfo dbInfo = new DbInfo(
                baseUrl,
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD));
        dbInfo.setDbType(ElasticsearchDialect.IDENTIFIER);
        return new ElasticsearchCatalog(
                context.getName(),
                dbInfo, null);
    }
}

