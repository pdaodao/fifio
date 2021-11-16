package com.github.apengda.fifio.kafka;

import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.kafka.catalog.KafkaCatalog;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.apengda.fifio.jdbc.catalog.MyJdbcCatalogFactoryOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

public class KafkaCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return KafkaDialect.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BASE_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        String baseUrl = helper.getOptions().get(BASE_URL);
        if (StringUtils.isBlank(baseUrl)) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            String.join("\n", BASE_URL.key())));
        }

        final Set<ConfigOption<?>> selfOptions = new HashSet<>();
        selfOptions.addAll(requiredOptions());
        selfOptions.addAll(optionalOptions());
        final Set<String> selfOptionsKeys = selfOptions.stream().map(t -> t.key()).collect(Collectors.toSet());
        final Map<String, String> options = new LinkedHashMap<>();
        final Set<String> tableFactoryOptions = new KafkaDynamicTableFactory()
                .optionalOptions().stream().map(t -> t.key()).collect(Collectors.toSet());

        for (Map.Entry<String, String> entry : context.getOptions().entrySet()) {
            if (tableFactoryOptions.contains(entry.getKey()) && !selfOptionsKeys.contains(entry.getKey())) {
                options.put(entry.getKey(), entry.getValue());
                continue;
            }
            if (!tableFactoryOptions.contains(entry.getKey()) && !selfOptionsKeys.contains(entry.getKey())) {
                String.format("Invalid option is:\n\n"
                        + "%s", entry.getKey());
            }
        }
        DbInfo dbInfo = new DbInfo(
                baseUrl,
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD));

        dbInfo.setDbType(KafkaDialect.IDENTIFIER);
        return new KafkaCatalog(
                context.getName(),
                dbInfo, options);
    }
}