package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.JdbcDialect;

import java.util.Optional;

public class PostgresDialect extends CommonMetaDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public boolean canHandle(String url) {
        if (url == null) {
            return false;
        }
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }
}
