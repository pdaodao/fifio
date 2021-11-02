package com.github.apengda.fifio.elasticsearch.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DbUtil {
    public static final String[] TABLE = {"TABLE"};
    public static final String[] VIEW = {"VIEW"};


    public static String buildUrl(final String url, String databaseName) {
        if (url == null) {
            return null;
        }
        if (databaseName == null || databaseName.length() < 1) {
            return url;
        }
        databaseName = databaseName.trim();
        String baseUrl = url;
        String baseUrlParams = "";
        if (url.contains("?")) {
            baseUrl = baseUrl.substring(0, baseUrl.indexOf("?"));
            baseUrlParams = baseUrl.substring(baseUrl.indexOf("?"));
        }
        String[] parts = baseUrl.trim().split("\\/+");
        if (parts.length == 2 || parts.length == 3) {
            baseUrl = parts[0] + "//" + parts[1] + "/" + databaseName;
        } else {
            throw new IllegalArgumentException(String.format("invalid jdbc connection url '%s'", url));
        }
        return baseUrl + baseUrlParams;
    }

    public static String getTableSchema(String tableName) {
        if (tableName == null) {
            return null;
        }
        tableName = tableName.trim();
        if (!tableName.contains(".")) {
            return null;
        }
        String[] paths = tableName.split("\\.");
        if (paths.length != 2) {
            throw new IllegalArgumentException(String.format("Table name '%s' is not valid. The parsed length is %d", tableName, paths.length));
        }
        return paths[0].trim();
    }

    public static String getTableName(String tableName) {
        if (tableName == null) {
            return null;
        }
        tableName = tableName.trim();
        if (!tableName.contains(".")) {
            return tableName;
        }
        String[] paths = tableName.split("\\.");
        if (paths.length != 2) {
            throw new IllegalArgumentException(String.format("Table name '%s' is not valid. The parsed length is %d", tableName, paths.length));
        }
        return paths[1].trim();
    }

    public static void print(ResultSet rs) {
        try {
            ResultSetMetaData rsMeta = rs.getMetaData();
            int count = rsMeta.getColumnCount();
            for (int i = 1; i <= count; i++) {
                System.out.print(rs.getMetaData().getColumnLabel(i) + "  ||  ");
            }
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= count; i++) {
                    System.out.print(rs.getString(i) + " || ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
