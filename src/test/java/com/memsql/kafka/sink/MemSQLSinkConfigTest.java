package com.memsql.kafka.sink;

import com.memsql.kafka.utils.DataCompression;
import com.memsql.kafka.utils.DataFormat;
import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MemSQLSinkConfigTest {

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

    private Map<String, String> getMinimalRequiredParameters() {
        return new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "ddlendpoint:3306");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testDb");
        }};
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
            add("host1:3306");
            add("host2");
        }};
        props.put(MemSQLSinkConfig.DML_ENDPOINTS, String.join(",", dmlEndpoints));
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dmlEndpoints, dmlEndpoints);
    }

    @Test
    public void successDmlDefaultEqualsDdlParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dmlEndpoints, Collections.singletonList(props.get(MemSQLSinkConfig.DDL_ENDPOINT)));
    }

    @Test
    public void successUserParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.CONNECTION_USER, "testUser123");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.user, props.get(MemSQLSinkConfig.CONNECTION_USER));
    }

    @Test
    public void successPasswordParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        props.put(MemSQLSinkConfig.CONNECTION_PASSWORD, "SuPerSECRETpASSWORD123");
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.password, props.get(MemSQLSinkConfig.CONNECTION_PASSWORD));
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
        }};
        props.putAll(tableKeys);
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.tableKeys.size(), tableKeys.size());
        for (int i = 0; i < config.tableKeys.size(); i++) {
            TableKey tableKey = config.tableKeys.get(i);
            tableKeys.keySet().forEach(key -> {
                String[] keyParts = key.split("\\.");
                if (keyParts[1].toUpperCase().equals(tableKey.type.name())) {
                    assertEquals(tableKeys.get(key), tableKey.columns);
                    if (!tableKey.name.isEmpty()) {
                        assertEquals(tableKey.name, key.split("\\.")[2]);
                    }
                }
            });
        }
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
        assertEquals(config.metadataTableName, "kafka-connect-transaction-metadata");

        props.put(MemSQLSinkConfig.METADATA_TABLE_NAME, "kafka-connect-new-table-name");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.metadataTableName, props.get(MemSQLSinkConfig.METADATA_TABLE_NAME));
    }

    @Test
    public void successDataFormatParameter() {
        Map<String, String> props = getMinimalRequiredParameters();
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.CSV);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "Csv");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.CSV);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "CSV");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.CSV);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "csv");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.CSV);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "Avro");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.AVRO);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "avro");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.AVRO);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "AVRO");
        config = new MemSQLSinkConfig(props);
        assertEquals(config.dataFormat, DataFormat.AVRO);
    }
}
