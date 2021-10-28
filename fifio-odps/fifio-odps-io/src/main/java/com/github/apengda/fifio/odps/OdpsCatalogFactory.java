package com.github.apengda.fifio.odps;

import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.odps.catalog.OdpsCatalog;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

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
        options.add(OdpsCatalogFactoryOptions.URL);
        options.add(OdpsCatalogFactoryOptions.PROJECT);
        options.add(OdpsCatalogFactoryOptions.USERNAME);
        options.add(OdpsCatalogFactoryOptions.PASSWORD);
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

        DbInfo dbInfo = new DbInfo(
                helper.getOptions().get(OdpsCatalogFactoryOptions.URL),
                helper.getOptions().get(OdpsCatalogFactoryOptions.USERNAME),
                helper.getOptions().get(OdpsCatalogFactoryOptions.PASSWORD)

        );
        dbInfo.setDbName(helper.getOptions().get(OdpsCatalogFactoryOptions.PROJECT));
        return new OdpsCatalog(context.getName(),
                helper.getOptions().get(OdpsCatalogFactoryOptions.PROJECT),
                dbInfo);
    }
}
