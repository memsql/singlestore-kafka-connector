package com.singlestore.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class VersionProvider {

    private static final Logger log = LoggerFactory.getLogger(VersionProvider.class);
    private static final String VERSION;
    private static final String KAFKA_VERSION;

    static {
        String versionProperty = "unknown";
        String kafkaVersionProperty = "unknown";
        try {
            Properties props = new Properties();
            props.load(VersionProvider.class.getResourceAsStream("/application.properties"));
            versionProperty = props.getProperty("version", versionProperty).trim();
            kafkaVersionProperty = props.getProperty("kafka.version", kafkaVersionProperty).trim();
        } catch (IOException ex) {
            log.warn("Error while loading version:", ex);
        }
        VERSION = versionProperty;
        KAFKA_VERSION = kafkaVersionProperty;
    }

    public static String getVersion() {
        return VERSION;
    }

    public static String getKafkaVersion() {
        return KAFKA_VERSION;
    }

    
}
