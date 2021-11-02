package com.github.apengda.fifio.base;

import org.apache.commons.lang3.time.FastDateFormat;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;

public class DateTimeUtil {
    public static TimeZone ShangHaiZone = TimeZone.getTimeZone("GMT+8");
    private static final FastDateFormat DATE_TIME_FORMATTER = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", ShangHaiZone);
    private static final FastDateFormat DATE_FORMATTER = FastDateFormat.getInstance("yyyy-MM-dd", ShangHaiZone);
    private static final FastDateFormat FULL_TIME_FORMATTER = FastDateFormat.getInstance("HH:mm:ss", ShangHaiZone);
    private static final FastDateFormat SHORT_TIME_FORMATTER = FastDateFormat.getInstance("HH:mm", ShangHaiZone);


    public static Date asDateTime(Object v) throws IOException {
        if (v == null) {
            return null;
        }
        if (v instanceof Date) {
            return (Date) v;
        }
        try {
            if (v instanceof String) {
                return DATE_TIME_FORMATTER.parse((String) v);
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        if (v instanceof Long) {
            return new Date((long) v);
        }
        if (v instanceof Timestamp) {
            return new Date(((Timestamp) v).getTime());
        }
        return null;
    }
}
