package com.github.apengda.fifio.odps.catalog;

import com.github.apengda.fifio.jdbc.catalog.AbstractDbMetaCatalog;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.Factory;

import java.util.Map;
import java.util.Optional;

public class OdpsCatalog extends AbstractDbMetaCatalog {

    public OdpsCatalog(String catalogName, String defaultDatabase, DbInfo dbInfo) {
        super(catalogName, defaultDatabase, dbInfo);
    }

    @Override
    protected Map<String, String> buildTableProps(ObjectPath tablePath) {
        return null;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.empty();
    }
}
