package com.github.apengda.fifio.elasticsearch.dialect;

import com.github.apengda.fifio.elasticsearch.DbMetaDialect;

import java.util.Optional;

public class PostgresDialect extends CommonMetaDialect implements DbMetaDialect {
    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }
}
