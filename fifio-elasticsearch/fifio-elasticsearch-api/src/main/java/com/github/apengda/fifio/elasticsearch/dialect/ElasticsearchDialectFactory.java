package com.github.apengda.fifio.elasticsearch.dialect;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;
import com.github.apengda.fifio.elasticsearch.DbMetaDialectFactory;

public class ElasticsearchDialectFactory implements DbMetaDialectFactory {

    @Override
    public boolean accept(String url, String typeName) {
        if (typeName == null) {
            return false;
        }
        return ElasticsearchDialect.IDENTIFIER.equalsIgnoreCase(typeName.trim());
    }

    @Override
    public DbMetaDialect create() {
        return new ElasticsearchDialect();
    }
}
