package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.DbMetaDialect;
import com.github.apengda.fifio.jdbc.DbMetaDialectFactory;

public class CommonMetaDialectFactory implements DbMetaDialectFactory {
    @Override
    public boolean accept(String url, String typeName) {
        if (url == null) {
            return false;
        }
        return url.startsWith("jdbc");
    }

    @Override
    public DbMetaDialect create() {
        return new CommonMetaDialect();
    }
}
