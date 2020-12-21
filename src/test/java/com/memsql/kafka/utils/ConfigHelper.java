package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLSinkConfig;

import java.util.HashMap;
import java.util.Map;

public class ConfigHelper {

    public static Map<String, String> getMinimalRequiredParameters() {
        return new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_PASSWORD, System.getenv("MEMSQL_PASSWORD"));
        }};
    }
}
