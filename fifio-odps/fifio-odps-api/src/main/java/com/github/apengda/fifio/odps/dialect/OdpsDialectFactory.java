package com.github.apengda.fifio.odps.dialect;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.DbMetaDialectFactory;

public class OdpsDialectFactory implements DbMetaDialectFactory {

    @Override
    public boolean accept(String url, String typeName) {
        if (typeName == null) {
            return false;
        }
        return "odps".equalsIgnoreCase(typeName.trim());
    }

    @Override
    public DbMetaDialect create() {
        return new OdpsDialect();
    }
}
