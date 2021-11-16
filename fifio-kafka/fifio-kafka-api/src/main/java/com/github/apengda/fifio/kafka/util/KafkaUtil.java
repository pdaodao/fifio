package com.github.apengda.fifio.kafka.util;

import com.github.apengda.fifio.base.JsonUtil;
import com.github.apengda.fifio.jdbc.frame.DbInfo;
import com.github.apengda.fifio.jdbc.frame.TableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaUtil {
    public static final Integer MAX_ROWS_FOR_META = 1000;

    public static Admin admin(final DbInfo dbInfo) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, dbInfo.getUrl());
        Admin admin = Admin.create(properties);
        return admin;
    }

    public static List<String> topicList(final DbInfo dbInfo) throws Exception {
        try (Admin admin = admin(dbInfo)) {
            return admin.listTopics(new ListTopicsOptions().timeoutMs(2000))
                    .names().get().stream().collect(Collectors.toList());
        }
    }

    public static List<String> recordListForMeta(final DbInfo dbInfo, final String topic) {
        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, dbInfo.getUrl());
        props.setProperty(CommonClientConfigs.GROUP_ID_CONFIG, "fif_meta");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final List<String> list = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            for (int i = 0; i < MAX_ROWS_FOR_META; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1500));
                if (records.count() < 1) {
                    break;
                }
                int size = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (size > MAX_ROWS_FOR_META) {
                        break;
                    }
                    list.add(record.value());
                }
            }
        }
        return list;
    }

    public static TableInfo guessTableInfo(String topic, List<String> records) throws Exception {
        if (records == null || records.size() < 1) {
            throw new IllegalArgumentException("kafka record is empty, cannot parse table info!");
        }
        final TableInfo tableInfo = new TableInfo(null, topic);
        final Map<String, TableInfo.TableColumn> map = new LinkedHashMap<>();
        for (String record : records) {
            if (StringUtils.isBlank(record)) {
                continue;
            }
            final Map<String, Object> row = JsonUtil.MAPPER.readValue(record, LinkedHashMap.class);
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                TableInfo.TableColumn old = map.get(entry.getKey());
                String oldType = old != null ? old.typeName : null;
                String typeName = guessType(entry.getValue(), oldType);
                map.put(entry.getKey(), new TableInfo.TableColumn(entry.getKey(), typeName));
            }
        }
        for (Map.Entry<String, TableInfo.TableColumn> entry : map.entrySet()) {
            if (StringUtils.isEmpty(entry.getValue().typeName)) {
                tableInfo.addColumn(new TableInfo.TableColumn(entry.getValue().name, "STRING"));
            } else {
                tableInfo.addColumn(entry.getValue());
            }
        }
        return tableInfo;
    }

    private static String guessType(Object val, String oldType) {
        if (val == null) {
            return oldType;
        }
        if (oldType != null && "STRING".equals(oldType)) {
            return oldType;
        }
        if (val instanceof String) {
            return "STRING";
        }
        if (val instanceof Float) {
            if ("DOUBLE".equals(oldType)) {
                return oldType;
            }
            return "FLOAT";
        }
        if (val instanceof Double) {
            return "DOUBLE";
        }
        if (val instanceof Integer || val instanceof Short) {
            if ("BIGINT".equals(oldType)
                    || "FLOAT".equals(oldType)
                    || "DOUBLE".equals(oldType)) {
                return oldType;
            }
            return "INT";
        }
        if (val instanceof Long) {
            if ("FLOAT".equals(oldType) || "DOUBLE".equals(oldType)) {
                return "DOUBLE";
            }
            return "BIGINT";
        }
        if (val instanceof Boolean) {
            if (oldType == null) {
                return "BOOLEAN";
            }
            return oldType;
        }
        throw new IllegalArgumentException("unknown kafka type: " + val.getClass().getName());
    }

}
