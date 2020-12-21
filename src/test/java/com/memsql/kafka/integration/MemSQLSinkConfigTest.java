package com.memsql.kafka.integration;

import com.memsql.kafka.sink.MemSQLSinkConfig;
import com.memsql.kafka.utils.DataCompression;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static com.memsql.kafka.utils.ConfigHelper.getMinimalRequiredParameters;
import static org.junit.Assert.*;

public class MemSQLSinkConfigTest extends IntegrationBase {

    @Test
    public void failWithEmptyMap() {
        try {
            new MemSQLSinkConfig(new HashMap<>());
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Missing required configuration \"connection.ddlEndpoint\" which has no default value.");
        }
    }

    @Test
    public void failWithoutDdlEndpoint() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "testDatabase");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Missing required configuration \"connection.ddlEndpoint\" which has no default value.");
        }
    }

    @Test
    public void failWithWrongDdlEndpoint() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "testDatabase");
            props.put(MemSQLSinkConfig.DDL_ENDPOINT, "wrong_host:5506");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Could not connect to address=(host=wrong_host)(port=5506)(type=master) : wrong_host");
        }
    }

    @Test
    public void failWithoutDatabase() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put(MemSQLSinkConfig.DDL_ENDPOINT, "ddlendpoint:3306");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Missing required configuration \"connection.database\" which has no default value.");
        }
    }

    @Test
    public void failWithWrongDatabase() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.CONNECTION_DATABASE, "wrongDatabase");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Unknown database 'wrongDatabase'");
        }
    }

    @Test
    public void successWithMinimalRequiredParameters() {
        Map<String, String> props = getMinimalRequiredParameters();
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.ddlEndpoint, props.get(MemSQLSinkConfig.DDL_ENDPOINT));
        assertEquals(config.database, props.get(MemSQLSinkConfig.CONNECTION_DATABASE));
    }

    @Test
    public void successDmlEndpointsParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        List<String> dmlEndpoints = new ArrayList<String>() {{
            add("localhost:5507");
            add("localhost:5506");
        }};
        props.put(MemSQLSinkConfig.DML_ENDPOINTS, String.join(",", dmlEndpoints));
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dmlEndpoints, dmlEndpoints);
    }

    @Test
    public void failWithWrongDmlEndpointsParameter() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            List<String> dmlEndpoints = new ArrayList<String>() {{
                add("wrong_host:5506");
            }};
            props.put(MemSQLSinkConfig.DML_ENDPOINTS, String.join(",", dmlEndpoints));
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch(ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Could not connect to address=(host=wrong_host)(port=5506)(type=master) : wrong_host");
        }
    }

    @Test
    public void successDmlDefaultEqualsDdlParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dmlEndpoints, Collections.singletonList(props.get(MemSQLSinkConfig.DDL_ENDPOINT)));
    }

    @Test
    public void successUserParameter() {
        try {
            executeQuery("DROP USER IF EXISTS test_user");
            executeQuery("CREATE USER test_user");
            executeQuery("GRANT ALL PRIVILEGES ON testdb.* TO test_user");

            Map<String, String> props = getMinimalRequiredParameters();
            props.remove(MemSQLSinkConfig.CONNECTION_PASSWORD);
            props.put(MemSQLSinkConfig.CONNECTION_USER, "test_user");
            MemSQLSinkConfig config = new MemSQLSinkConfig(props);
            assertEquals(config.user, props.get(MemSQLSinkConfig.CONNECTION_USER));
        } catch(Exception ex) {
            log.error("", ex);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void failWrongUserParameter() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.CONNECTION_USER, "wrong_user");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch(Exception ex) {
            assertTrue(ex.getLocalizedMessage().startsWith("Access denied for user 'wrong_user'@'172.17.0.1'"));
        }
    }

    @Test
    public void successPasswordParameter() {
        try {
            executeQuery("DROP USER IF EXISTS test_user");
            executeQuery("CREATE USER test_user IDENTIFIED BY 'SuPerSECRETpASSWORD123'");
            executeQuery("GRANT ALL PRIVILEGES ON testdb.* TO test_user");

            Map<String, String> props = getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.CONNECTION_USER, "test_user");
            props.put(MemSQLSinkConfig.CONNECTION_PASSWORD, "SuPerSECRETpASSWORD123");
            MemSQLSinkConfig config = new MemSQLSinkConfig(props);
            assertEquals(config.password, props.get(MemSQLSinkConfig.CONNECTION_PASSWORD));
        } catch(Exception ex) {
            log.error("", ex);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void failWrongPasswordParameter() {
        try {
            executeQuery("DROP USER IF EXISTS test_user");
            executeQuery("CREATE USER test_user IDENTIFIED BY 'SuPerSECRETpASSWORD123'");
            executeQuery("GRANT ALL PRIVILEGES ON testdb.* TO test_user");

            Map<String, String> props = getMinimalRequiredParameters();
            props.put(MemSQLSinkConfig.CONNECTION_USER, "test_user");
            props.put(MemSQLSinkConfig.CONNECTION_PASSWORD, "wRongSuPerSECRETpASSWORD123");
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch(Exception ex) {
            assertEquals(ex.getLocalizedMessage(), "Access denied for user 'test_user'@'172.17.0.1' (using password: YES)");
        }
    }

    @Test
    public void successSqlParams() {
        Map<String, String> props = getMinimalRequiredParameters();
        Map<String, String> sqlParams = new HashMap<String, String>() {{
            put("params.sslEnable", "true");
            put("params.option1", "someValue");
            put("params.secondOption", "another-value");
        }};
        props.putAll(sqlParams);
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        sqlParams.keySet().forEach(k -> {
            String key = k.substring("params.".length());
            assertTrue(config.sqlParams.containsKey(key));
            assertEquals(config.sqlParams.get(key), sqlParams.get(k));
        });
    }

    @Test
    public void successMaxRetriesParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.MAX_RETRIES, "100");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.maxRetries, Integer.parseInt(props.get(MemSQLSinkConfig.MAX_RETRIES)));
    }

    @Test
    public void successRetryBackoffMsParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.RETRY_BACKOFF_MS, "5000");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.retryBackoffMs, Integer.parseInt(props.get(MemSQLSinkConfig.RETRY_BACKOFF_MS)));
    }

    @Test
    public void successDataCompressionParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "gzip");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "GZIP");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "Gzip");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "lz4");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "LZ4");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "Lz4");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "SKIP");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "Skip");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "skip");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);
    }

    @Test
    public void failDataCompressionParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "someDataFormat");
        try {
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Configuration \"memsql.loadDataCompression\" is wrong. Available options: Gzip, LZ4, Skip");
        }
    }

    @Test
    public void successTableKeysParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        Map<String, String> tableKeys = new HashMap<String, String>() {{
            put("tableKey.primary", "column1");
            put("tableKey.columnStore.name", "column2");
            put("tableKey.uniQUE", "column3");
            put("tableKey.shaRd", "another-column");
            put("tableKey.key.SOME-name", "weIrD_ColUMN");
            put("tableKey.key", "`col1 ,,,```   ,      ```,`, asd");
            put("tableKey.key.name1.name2", "`col1 ,,,```   ,      ```,`, asd");
        }};
        props.putAll(tableKeys);
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);

        Set<String> keys = config.tableKeys.stream().map(TableKey::toString).collect(Collectors.toSet());
        assertEquals(keys,
                new HashSet<>(Arrays.asList(
                        "PRIMARY KEY (`column1`)",
                        "SHARD KEY (`another-column`)",
                        "KEY `name`(`column2`) USING CLUSTERED COLUMNSTORE",
                        "KEY (`col1 ,,,```, ```,`, `asd`)",
                        "KEY `name1.name2`(`col1 ,,,```, ```,`, `asd`)",
                        "KEY `SOME-name`(`weIrD_ColUMN`)",
                        "UNIQUE KEY (`column3`)"
                )));
    }

    @Test
    public void failTableKeysParameter() {

        List<String> keysToCheck = new ArrayList<String>() {{
            add("tableKey.primarry.name");
            add("tableKey.columStore");
            add("tableKey.uniQUu");
            add("tableKey.shad");
            add("tableKey.keys");
            add("tableKey.something");
        }};
        keysToCheck.forEach(this::testTableKeyParameter);
    }

    private void testTableKeyParameter(String key) {
        Map<String, String> props = getMinimalRequiredParameters();
        Map<String, String> tableKeys = new HashMap<String, String>() {{
            put(key, "column1");
        }};
        props.putAll(tableKeys);
        try {
            new MemSQLSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(),
                    String.format("Option '%s' must specify an index type from the following options: [PRIMARY, COLUMNSTORE, UNIQUE, SHARD, KEY]", key));
        }
    }

    @Test
    public void successMetadataTableAllowParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "true");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertTrue(config.metadataTableAllow);

        props.put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        config = new MemSQLSinkConfig(props);
        assertFalse(config.metadataTableAllow);
    }

    @Test
    public void successMetadataTableNameParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.metadataTableName, "kafka_connect_transaction_metadata");

        props.put(MemSQLSinkConfig.METADATA_TABLE_NAME, "kafka_connect_new_table_name");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.metadataTableName, props.get(MemSQLSinkConfig.METADATA_TABLE_NAME));
    }
}
