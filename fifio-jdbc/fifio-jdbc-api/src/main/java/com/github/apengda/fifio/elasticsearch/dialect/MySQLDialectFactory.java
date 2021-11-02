package com.github.apengda.fifio.elasticsearch.dialect;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.DbMetaDialectFactory;

public class MySQLDialectFactory implements DbMetaDialectFactory {
    @Override
    public boolean accept(String url, String typeName) {
        if (url == null) {
            return false;
        }
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public DbMetaDialect create() {
        return new MySQLDialect();
    }
}
