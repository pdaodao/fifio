package com.github.apengda.fifio.jdbc;

import com.github.apengda.fifio.jdbc.dialect.CommonMetaDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public final class DbMetaDialectLoader {

    private static final Logger logger = LoggerFactory.getLogger(DbMetaDialectLoader.class);

    private DbMetaDialectLoader() {
    }


    public static DbMetaDialect load(final String url, final String type) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        List<DbMetaDialectFactory> foundFactories = discoverFactories(cl);

        if (foundFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any db meta dialect factories that implement '%s' in the classpath.",
                            DbMetaDialectFactory.class.getName()));
        }
        final List<DbMetaDialectFactory> matchingFactories =
                foundFactories.stream().filter(f -> f.accept(url, type)).collect(Collectors.toList());

        if (matchingFactories.isEmpty() && url != null && url.startsWith("jdbc")) {
            logger.info("no db meta dialect factory found for '%s' use common Dialect", url);
            return new CommonMetaDialect();
        }

        if (matchingFactories.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Could not find any db meta dialect factory that can handle url '%s'  type '%s'",
                            url, type));
        }
        if (matchingFactories.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple db meta dialect factories can handle url '%s'  type '%s'.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            url, type, matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingFactories.get(0).create();
    }

    private static List<DbMetaDialectFactory> discoverFactories(ClassLoader classLoader) {
        try {
            final List<DbMetaDialectFactory> result = new LinkedList<>();
            ServiceLoader.load(DbMetaDialectFactory.class, classLoader)
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            logger.error("Could not load service provider for db meta dialects factory.", e);
            throw new RuntimeException(
                    "Could not load service provider for db meta dialects factory.", e);
        }
    }

}
