package com.singlestore.kafka.utils;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;

import java.util.HashMap;
import java.util.Map;

public class ConfigHelper {

    public static Map<String, String> getMinimalRequiredParameters() {
        return new HashMap<String, String>() {{
            put(SingleStoreSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testdb");
            put(SingleStoreSinkConfig.CONNECTION_PASSWORD, System.getenv("SINGLESTORE_PASSWORD"));
        }};
    }
}
