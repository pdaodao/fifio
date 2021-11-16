package com.github.apengda.fifio.jdbc.dialect;

import com.github.apengda.fifio.jdbc.DbMetaDialect;

import java.util.Optional;

public class MySQLDialect extends CommonMetaDialect implements DbMetaDialect {
    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public Integer fetchSize() {
        return Integer.MAX_VALUE;
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
