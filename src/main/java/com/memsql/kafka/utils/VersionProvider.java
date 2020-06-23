package com.memsql.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class VersionProvider {

    private static final Logger log = LoggerFactory.getLogger(VersionProvider.class);
    private static final String VERSION;

    static {
        String versionProperty = "unknown";
        try {
            Properties props = new Properties();
            props.load(VersionProvider.class.getResourceAsStream("/application.properties"));
            versionProperty = props.getProperty("version", versionProperty).trim();
        } catch (IOException ex) {
            log.warn("Error while loading version:", ex);
        }
        VERSION = versionProperty;
    }

    public static String getVersion() {
        return VERSION;
    }
}
