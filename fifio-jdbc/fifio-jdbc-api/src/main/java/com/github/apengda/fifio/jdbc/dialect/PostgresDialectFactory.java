package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.DbMetaDialect;
import com.github.apengda.fifio.jdbc.DbMetaDialectFactory;

public class PostgresDialectFactory implements DbMetaDialectFactory {
    @Override
    public boolean accept(String url, String typeName) {
        if (url == null) {
            return false;
        }
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public DbMetaDialect create() {
        return new PostgresDialect();
    }
}
