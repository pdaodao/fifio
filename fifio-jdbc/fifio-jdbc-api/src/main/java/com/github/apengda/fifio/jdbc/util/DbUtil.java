package com.github.apengda.fifio.jdbc.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class DbUtil {
    public static final String[] TABLE = {"TABLE"};
    public static final String[] VIEW = {"VIEW"};

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
