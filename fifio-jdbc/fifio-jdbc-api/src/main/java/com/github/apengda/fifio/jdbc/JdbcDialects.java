package com.github.apengda.fifio.jdbc;

import com.github.apengda.fifio.jdbc.dialect.CommonMetaDialect;
import com.github.apengda.fifio.jdbc.dialect.MySQLDialect;
import com.github.apengda.fifio.jdbc.dialect.PostgresDialect;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class JdbcDialects {
    public static final String[] TABLE = {"TABLE"};
    public static final String[] VIEW = {"VIEW"};

    private static final List<JdbcDialect> DIALECTS =
            Arrays.asList(
                    new MySQLDialect(),
                    new PostgresDialect(),
                    new CommonMetaDialect());

    public static Optional<JdbcDialect> get(String url) {
        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.canHandle(url)) {
                return Optional.of(dialect);
            }
        }
        return Optional.empty();
    }
}
