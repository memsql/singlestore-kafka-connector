package com.singlestore.kafka.integration;

import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.utils.DataCompression;
import com.singlestore.kafka.utils.TableKey;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static com.singlestore.kafka.utils.ConfigHelper.getMinimalRequiredParameters;
import static org.junit.Assert.*;

public class SingleStoreSinkConfigTest extends IntegrationBase {

    @Test
    public void failWithWrongDdlEndpoint() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testDatabase");
            props.put(SingleStoreSinkConfig.DDL_ENDPOINT, "wrong_host:5506");
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Socket fail to connect to host:address=(host=wrong_host)(port=5506)(type=primary). wrong_host");
        }
    }

    @Test
    public void failWithoutDatabase() {
        try {
            Map<String, String> props = new HashMap<>();
            props.put(SingleStoreSinkConfig.DDL_ENDPOINT, "ddlendpoint:3306");
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Missing required configuration \"connection.database\" which has no default value.");
        }
    }

    @Test
    public void failWithWrongDatabase() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.CONNECTION_DATABASE, "wrongDatabase");
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertTrue(ex.getLocalizedMessage().contains("Unknown database 'wrongDatabase'"));
        }
    }

    @Test
    public void successWithMinimalRequiredParameters() {
        Map<String, String> props = getMinimalRequiredParameters();
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.ddlEndpoint, props.get(SingleStoreSinkConfig.DDL_ENDPOINT));
        assertEquals(config.database, props.get(SingleStoreSinkConfig.CONNECTION_DATABASE));
    }

    @Test
    public void successDmlEndpointsParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        List<String> dmlEndpoints = new ArrayList<String>() {{
            add("localhost:5507");
            add("localhost:5506");
        }};
        props.put(SingleStoreSinkConfig.DML_ENDPOINTS, String.join(",", dmlEndpoints));
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.dmlEndpoints, dmlEndpoints);
    }

    @Test
    public void failWithWrongDmlEndpointsParameter() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            List<String> dmlEndpoints = new ArrayList<String>() {{
                add("wrong_host:5506");
            }};
            props.put(SingleStoreSinkConfig.DML_ENDPOINTS, String.join(",", dmlEndpoints));
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch(ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Socket fail to connect to host:address=(host=wrong_host)(port=5506)(type=primary). wrong_host");
        }
    }

    @Test
    public void successDmlDefaultEqualsDdlParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.dmlEndpoints, Collections.singletonList(props.get(SingleStoreSinkConfig.DDL_ENDPOINT)));
    }

    @Test
    public void successClientEndpointParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.CLIENT_ENDPOINT, props.get(SingleStoreSinkConfig.DDL_ENDPOINT));
        props.remove(SingleStoreSinkConfig.DDL_ENDPOINT);
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.ddlEndpoint, props.get(SingleStoreSinkConfig.CLIENT_ENDPOINT));
        assertEquals(config.dmlEndpoints, Collections.singletonList(props.get(SingleStoreSinkConfig.CLIENT_ENDPOINT)));
    }

    @Test
    public void failClientEndpointAndDdlEndpointParameters() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.CLIENT_ENDPOINT, props.get(SingleStoreSinkConfig.DDL_ENDPOINT));

        try {
            new SingleStoreSinkConfig(props);
        } catch(ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Configurations \"singlestore.connection.ddlEndpoint\" and \"singlestore.connection.clientEndpoint\" are mutually exclusive");
        }
    }

    @Test
    public void failClientEndpointAndDmlEndpointsParameters() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.CLIENT_ENDPOINT, props.get(SingleStoreSinkConfig.DDL_ENDPOINT));
        props.put(SingleStoreSinkConfig.DML_ENDPOINTS, props.get(SingleStoreSinkConfig.DDL_ENDPOINT));
        props.remove(SingleStoreSinkConfig.DDL_ENDPOINT);

        try {
            new SingleStoreSinkConfig(props);
        } catch(ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Configurations \"singlestore.connection.dmlEndpoints\" and \"singlestore.connection.clientEndpoint\" are mutually exclusive");
        }
    }

    @Test
    public void failNoEndpointParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.remove(SingleStoreSinkConfig.DDL_ENDPOINT);

        try {
            new SingleStoreSinkConfig(props);
        } catch(ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Either the \"singlestore.connection.ddlEndpoint\" or the \"singlestore.connection.clientEndpoint\" must be specified");
        }
    }

    @Test
    public void successUserParameter() {
        try {
            executeQuery("DROP USER IF EXISTS test_user");
            executeQuery("CREATE USER test_user");
            executeQuery("GRANT ALL PRIVILEGES ON testdb.* TO test_user");

            Map<String, String> props = getMinimalRequiredParameters();
            props.remove(SingleStoreSinkConfig.CONNECTION_PASSWORD);
            props.put(SingleStoreSinkConfig.CONNECTION_USER, "test_user");
            SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
            assertEquals(config.user, props.get(SingleStoreSinkConfig.CONNECTION_USER));
        } catch(Exception ex) {
            log.error("", ex);
            fail("Should not have thrown any exception");
        }
    }

    @Test
    public void failWrongUserParameter() {
        try {
            Map<String, String> props = getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.CONNECTION_USER, "wrong_user");
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch(Exception ex) {
            assertTrue(ex.getLocalizedMessage().contains("Access denied for user 'wrong_user'@'172.17.0.1'"));
        }
    }

    @Test
    public void successPasswordParameter() {
        try {
            executeQuery("DROP USER IF EXISTS test_user");
            executeQuery("CREATE USER test_user IDENTIFIED BY 'SuPerSECRETpASSWORD123'");
            executeQuery("GRANT ALL PRIVILEGES ON testdb.* TO test_user");

            Map<String, String> props = getMinimalRequiredParameters();
            props.put(SingleStoreSinkConfig.CONNECTION_USER, "test_user");
            props.put(SingleStoreSinkConfig.CONNECTION_PASSWORD, "SuPerSECRETpASSWORD123");
            SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
            assertEquals(config.password, props.get(SingleStoreSinkConfig.CONNECTION_PASSWORD));
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
            props.put(SingleStoreSinkConfig.CONNECTION_USER, "test_user");
            props.put(SingleStoreSinkConfig.CONNECTION_PASSWORD, "wRongSuPerSECRETpASSWORD123");
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch(Exception ex) {
            assertTrue(ex.getLocalizedMessage().contains( "Access denied for user 'test_user'@'172.17.0.1' (using password: YES)"));
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
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        sqlParams.keySet().forEach(k -> {
            String key = k.substring("params.".length());
            assertTrue(config.sqlParams.containsKey(key));
            assertEquals(config.sqlParams.get(key), sqlParams.get(k));
        });
    }

    @Test
    public void successMaxRetriesParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.MAX_RETRIES, "100");
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.maxRetries, Integer.parseInt(props.get(SingleStoreSinkConfig.MAX_RETRIES)));
    }

    @Test
    public void successRetryBackoffMsParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.RETRY_BACKOFF_MS, "5000");
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.retryBackoffMs, Integer.parseInt(props.get(SingleStoreSinkConfig.RETRY_BACKOFF_MS)));
    }

    @Test
    public void successDataCompressionParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "gzip");
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "GZIP");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "Gzip");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.gzip);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "lz4");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "LZ4");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "Lz4");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.lz4);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "SKIP");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "Skip");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "skip");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.dataCompression, DataCompression.skip);
    }

    @Test
    public void failDataCompressionParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "someDataFormat");
        try {
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(), "Configuration \"singlestore.loadDataCompression\" is wrong. Available options: Gzip, LZ4, Skip");
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
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);

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
            new SingleStoreSinkConfig(props);
            fail("Exception should be thrown");
        } catch (ConfigException ex) {
            assertEquals(ex.getLocalizedMessage(),
                    String.format("Option '%s' must specify an index type from the following options: [PRIMARY, COLUMNSTORE, UNIQUE, SHARD, KEY]", key));
        }
    }

    @Test
    public void successMetadataTableAllowParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "true");
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertTrue(config.metadataTableAllow);

        props.put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
        config = new SingleStoreSinkConfig(props);
        assertFalse(config.metadataTableAllow);
    }

    @Test
    public void successMetadataTableNameParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertEquals(config.metadataTableName, "kafka_connect_transaction_metadata");

        props.put(SingleStoreSinkConfig.METADATA_TABLE_NAME, "kafka_connect_new_table_name");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.metadataTableName, props.get(SingleStoreSinkConfig.METADATA_TABLE_NAME));
    }

    @Test
    public void successFilterParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        assertNull(config.filter);

        props.put(SingleStoreSinkConfig.FILTER, "a = 1");
        config = new SingleStoreSinkConfig(props);
        assertEquals(config.filter, props.get(SingleStoreSinkConfig.FILTER));
    }
}
