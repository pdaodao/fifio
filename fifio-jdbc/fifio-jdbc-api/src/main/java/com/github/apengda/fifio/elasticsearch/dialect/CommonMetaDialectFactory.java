package com.github.apengda.fifio.elasticsearch.dialect;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.DbMetaDialectFactory;

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
