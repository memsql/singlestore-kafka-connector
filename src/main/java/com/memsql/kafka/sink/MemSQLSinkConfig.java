package com.memsql.kafka.sink;

import com.memsql.kafka.utils.TableKey;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.*;

public class MemSQLSinkConfig extends AbstractConfig {
    private static final String CONNECTION_GROUP = "Connection";
    private static final String RETRY_GROUP = "Retry";
    private static final String MEMSQL_GROUP = "MemSQL";

    public static final String DDL_ENDPOINT = "connection.ddlEndpoint";
    private static final String DDL_ENDPOINT_DOC =
            "Hostname or IP address of the MemSQL Master Aggregator in the format `host[:port]`";
    private static final String DDL_ENDPOINT_DISPLAY = "DDL Endpoint";

    public static final String DML_ENDPOINTS = "connection.dmlEndpoints";
    private static final String DML_ENDPOINTS_DOC =
            "Hostname or IP address of MemSQL Aggregator nodes to run queries against " +
                    "in the format host[:port],host[:port],... (port is optional, multiple hosts separated by comma). " +
                    "Example: child-agg:3308,child-agg2 (default: ddlEndpoint)";
    private static final String DML_ENDPOINTS_DISPLAY = "DML Endpoints";

    public static final String CONNECTION_DATABASE = "connection.database";
    private static final String CONNECTION_DATABASE_DOC = "MemSQL connection database.";
    private static final String CONNECTION_DATABASE_DISPLAY = "MemSQL Database";

    public static final String CONNECTION_USER = "connection.user";
    private static final String CONNECTION_USER_DOC = "MemSQL connection user.";
    private static final String CONNECTION_USER_DISPLAY = "MemSQL User";

    public static final String CONNECTION_PASSWORD = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "MemSQL connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "MemSQL Password";

    public static final String SQL_PARAMETERS = "params.<value>";
    private static final String SQL_PARAMETERS_DOC = "Specify a specific MySQL or JDBC parameter which will be injected into the connection URI";
    private static final String SQL_PARAMETERS_DISPLAY = "Additional SQL Parameters";

    public static final String MAX_RETRIES = "max.retries";
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String LOAD_DATA_COMPRESSION = "memsql.loadDataCompression";
    private static final String LOAD_DATA_COMPRESSION_DOC = "Compress data on load; one of (GZip, LZ4, Skip) (default: GZip)";
    private static final String LOAD_DATA_COMPRESSION_DISPLAY = "MemSQL Load Data Compression";

    private static  final String TABLE_KEY = "tableKey.<index_type>[.<name>]";
    private static final String  TABLE_KEY_DOCS = "Specify additional keys to add to tables created by the connector";
    private static final String TABLE_KEY_DISPLAY = "Table key";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    DDL_ENDPOINT,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    DDL_ENDPOINT_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.LONG,
                    DDL_ENDPOINT_DISPLAY
            )
            .define(
                    DML_ENDPOINTS,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    DML_ENDPOINTS_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.LONG,
                    DML_ENDPOINTS_DISPLAY
            )
            .define(
                    CONNECTION_DATABASE,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_DATABASE_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_DATABASE_DISPLAY
            )
            .define(
                    CONNECTION_USER,
                    ConfigDef.Type.STRING,
                    "root",
                    ConfigDef.Importance.HIGH,
                    CONNECTION_USER_DOC,
                    CONNECTION_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_USER_DISPLAY
            )
            .define(
                    CONNECTION_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_PASSWORD_DOC,
                    CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_PASSWORD_DISPLAY
            )
            .define(
                    SQL_PARAMETERS,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    SQL_PARAMETERS_DOC,
                    CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.MEDIUM,
                    SQL_PARAMETERS_DISPLAY
            )
            .define(TABLE_KEY,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    TABLE_KEY_DOCS,
                    CONNECTION_GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    TABLE_KEY_DISPLAY
            )
            .define(MAX_RETRIES,
                    ConfigDef.Type.INT,
                    10,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    MAX_RETRIES_DOC,
                    RETRY_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY)
            .define(RETRY_BACKOFF_MS,
                    ConfigDef.Type.INT,
                    3000,
                    NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC,
                    RETRY_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    RETRY_BACKOFF_MS_DISPLAY)
            .define(LOAD_DATA_COMPRESSION,
                    ConfigDef.Type.STRING,
                    "GZip",
                    ConfigDef.Importance.LOW,
                    LOAD_DATA_COMPRESSION_DOC,
                    MEMSQL_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    LOAD_DATA_COMPRESSION_DISPLAY);

    public final String ddlEndpoint;
    public final List<String> dmlEndpoints;
    public final String database;
    public final String user;
    public final String password;
    public final Map<String, String> sqlParams;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final String dataCompression;
    public final List<TableKey> tableKeys;

    public MemSQLSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.ddlEndpoint = getString(DDL_ENDPOINT);
        this.dmlEndpoints = getDmlEndpoints();
        this.database = getString(CONNECTION_DATABASE);
        this.user = getString(CONNECTION_USER);
        this.password = getPasswordValue(CONNECTION_PASSWORD);
        this.sqlParams = getSqlParams(props);
        this.maxRetries = getInt(MAX_RETRIES);
        this.retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        this.dataCompression = getString(LOAD_DATA_COMPRESSION);
        this.tableKeys = getTableKeys(props);
    }

    private Map<String, String> getSqlParams(Map<String, String> props) {
        String paramsPrefix = "params.";
        Map<String, String> sqlParams = new HashMap<>();
        props.keySet().stream()
                .filter(key -> (key).startsWith(paramsPrefix))
                .forEach(key -> sqlParams.put((key).substring(paramsPrefix.length()), getString(key)));
        return sqlParams;
    }

    private List<TableKey> getTableKeys(Map<String, String> props) {
        String tableKeysPrefix = "tableKey.";
        List<TableKey> tableKeys = new ArrayList<>();
        props.keySet().stream()
                .filter(key -> key.startsWith(tableKeysPrefix))
                .forEach(
                        key -> {
                            String[]keyParts = key.split("\\.");
                            if (keyParts.length < 2) {
                                throw new ConnectException(
                                        String.format("Options starting with '%s.' must be formatted correctly. The key should be in the form `%s<index_type>[.<name>]`.", tableKeysPrefix, tableKeysPrefix)
                                );
                            }

                            TableKey.Type keyType;
                            try {
                                keyType = TableKey.Type.valueOf(keyParts[1].toUpperCase());
                            } catch(IllegalArgumentException ex) {
                                throw new ConnectException(
                                        String.format("Option '%s' must specify an index type from the following options: %s", key, Arrays.toString(TableKey.Type.values()))
                                );
                            }

                            String name = "";
                            if (keyParts.length == 3) {
                                name = keyParts[2];
                            }

                            tableKeys.add(new TableKey(keyType, name, props.get(key)));
                        }
                );
        return tableKeys;
    }

    private List<String> getDmlEndpoints() {
        List<String> dmlEndpoints = getList(DML_ENDPOINTS);
        if (dmlEndpoints == null || dmlEndpoints.isEmpty()) {
            return Collections.singletonList(getString(DDL_ENDPOINT));
        }
        return dmlEndpoints;
    }

    private String getPasswordValue(String key) {
        Password password = getPassword(key);
        if (password != null) {
            return password.value();
        }
        return null;
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
