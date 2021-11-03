package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreDbWriter;
import com.singlestore.kafka.sink.SingleStoreDialect;
import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.sink.SingleStoreSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class IntegrationBase {
    protected static final Logger log = LoggerFactory.getLogger(SingleStoreDbWriter.class);

    public static Connection jdbcConnection;

    @BeforeClass
    public static void setupDatabase() throws SQLException {
        // override global JVM timezone to GMT
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        Properties connProperties = new Properties();
        connProperties.put("user", "root");
        String password;
        if ((password = System.getenv("SINGLESTORE_PASSWORD")) != null) {
            connProperties.put("password", password);
        }
        jdbcConnection = DriverManager.getConnection("jdbc:mysql://localhost:5506/memsql",
                connProperties);

        // make singlestore use less memory
        executeQuery("SET GLOBAL default_partitions_per_leaf = 2");

        executeQuery("DROP DATABASE IF EXISTS testdb");
        executeQuery("CREATE DATABASE testdb");
    }

    public static void executeQuery(String sql) throws SQLException{
        log.trace("Executing SQL:\n{}", sql);
        try (Statement stmt = jdbcConnection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public static ResultSet executeQueryWithResultSet(String sql) throws SQLException{
        log.trace("Executing SQL:\n{}", sql);
        try (Statement stmt = jdbcConnection.createStatement()) {
            return stmt.executeQuery(sql);
        }
    }

    public static void put(Map<String, String> props, List<SinkRecord> records) throws SQLException {
        props.put(SingleStoreSinkConfig.DDL_ENDPOINT, "localhost:5506");
        props.put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testdb");
        String password;
        if ((password = System.getenv("SINGLESTORE_PASSWORD")) != null) {
            props.put(SingleStoreSinkConfig.CONNECTION_PASSWORD, password);
        }
        props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");

        executeQuery(String.format("DROP TABLE IF EXISTS testdb.%s", SingleStoreDialect.quoteIdentifier(records.iterator().next().topic())));

        SingleStoreSinkTask task = new SingleStoreSinkTask();
        task.start(props);
        task.put(records);
        task.stop();
    }
}
