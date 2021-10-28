package com.github.apengda.fifio.odps;

import com.github.apengda.fifio.jdbc.catalog.MyJdbcCatalog;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.util.DbUtil;
import com.github.apengda.fifio.odps.catalog.OdpsCatalog;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.github.apengda.fifio.jdbc.catalog.MyJdbcCatalogFactoryOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

public class OdpsCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return "odps";
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
                baseUrl,
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD));
        dbInfo.setDbType("odps");
        dbInfo.setDbName(defaultDbName);

        return new OdpsCatalog(
                context.getName(),
                defaultDbName,
                dbInfo);
    }
}
