package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.DbMetaDialect;

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
