package com.github.apengda.fifio.elasticsearch;

public interface DbMetaDialectFactory {

    boolean accept(String url, String typeName);

    DbMetaDialect create();
}
