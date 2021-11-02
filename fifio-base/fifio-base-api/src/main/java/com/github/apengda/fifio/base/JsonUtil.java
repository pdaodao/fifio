package com.github.apengda.fifio.base;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
    public static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
    }
}
