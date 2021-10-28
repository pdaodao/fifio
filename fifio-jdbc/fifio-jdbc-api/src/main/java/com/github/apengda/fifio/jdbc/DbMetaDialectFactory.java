package com.github.apengda.fifio.jdbc;

public interface DbMetaDialectFactory {

    boolean accept(String url, String typeName);

    DbMetaDialect create();
}
