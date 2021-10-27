package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.JdbcDialect;

import java.util.Optional;

public class MySQLDialect extends CommonMetaDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public Integer fetchSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean canHandle(String url) {
        if (url == null) {
            return false;
        }
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
