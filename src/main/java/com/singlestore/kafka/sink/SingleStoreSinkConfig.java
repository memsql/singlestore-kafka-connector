package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.DataCompression;
import com.singlestore.kafka.utils.JdbcHelper;
import com.singlestore.kafka.utils.TableKey;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class SingleStoreSinkConfig extends AbstractConfig {
    private static final String CONNECTION_GROUP = "Connection";
    private static final String RETRY_GROUP = "Retry";
    private static final String SINGLESTORE_GROUP = "SingleStore";
    private static final String DATAMAPPING_GROUP = "Data Mapping";

    public static final String DDL_ENDPOINT = "connection.ddlEndpoint";
    private static final String DDL_ENDPOINT_DOC =
            "Hostname or IP address of the SingleStore Master Aggregator in the format `host[:port]`";
    private static final String DDL_ENDPOINT_DISPLAY = "DDL Endpoint";

    public static final String DML_ENDPOINTS = "connection.dmlEndpoints";
    private static final String DML_ENDPOINTS_DOC =
            "Hostname or IP address of SingleStore Aggregator nodes to run queries against " +
                    "in the format host[:port],host[:port],... (port is optional, multiple hosts separated by comma). " +
                    "Example: child-agg:3308,child-agg2 (default: ddlEndpoint)";
    private static final String DML_ENDPOINTS_DISPLAY = "DML Endpoints";

    public static final String CONNECTION_DATABASE = "connection.database";
    private static final String CONNECTION_DATABASE_DOC = "SingleStore connection database.";
    private static final String CONNECTION_DATABASE_DISPLAY = "SingleStore Database";

    public static final String CONNECTION_USER = "connection.user";
    private static final String CONNECTION_USER_DOC = "SingleStore connection user.";
    private static final String CONNECTION_USER_DISPLAY = "SingleStore User";

    public static final String CONNECTION_PASSWORD = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "SingleStore connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "SingleStore Password";

    public static final String SQL_PARAMETERS = "params.<value>";
    private static final String SQL_PARAMETERS_DOC = "Specify a specific MySQL or JDBC parameter which will be injected into the connection URI";
    private static final String SQL_PARAMETERS_DISPLAY = "Additional SQL Parameters";

    public static final String TABLE_KEY = "tableKey.<index_type>[.<name>]";
    private static final String TABLE_KEY_DOCS = "Specify additional keys to add to tables created by the connector; value of this property is the comma separated list with names of the columns to apply key; <index_type> one of (`PRIMARY`, `COLUMNSTORE`, `UNIQUE`, `SHARD`, `KEY`)";
    private static final String TABLE_KEY_DISPLAY = "Table key";

    public static final String FIELDS_WHITELIST = "fields.whitelist";
    private static final String FIELDS_WHITELIST_DOCS = "Specify fields to be inserted to the database. By default all keys will be used; value of this property is the comma separated list with names of the columns`)";
    private static final String FIELDS_WHITELIST_DISPLAY = "Fields whitelist";

    public static final String MAX_RETRIES = "max.retries";
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String LOAD_DATA_COMPRESSION = "singlestore.loadDataCompression";
    private static final String LOAD_DATA_COMPRESSION_DOC = "Compress data on load; one of (GZip, LZ4, Skip) (default: GZip)";
    private static final String LOAD_DATA_COMPRESSION_DISPLAY = "SingleStore Load Data Compression";

    public static final String METADATA_TABLE_ALLOW = "singlestore.metadata.allow";
    private static final String METADATA_TABLE_ALLOW_DOCS = "Allows or denies the use of an additional meta-table to save the recording results (default: true)";
    private static final String METADATA_TABLE_ALLOW_DISPLAY = "Allow metadata store";

    public static final String METADATA_TABLE_NAME = "singlestore.metadata.table";
    private static final String METADATA_TABLE_NAME_DOCS = "Specify the name of an additional meta-table to save the recording results " +
                                                            "(default: `kafka-connect-transaction-metadata`)";
    private static final String METADATA_TABLE_NAME_DISPLAY = "Metadata table name";

    public static final String TABLE_NAME = "singlestore.tableName.<topicName>";
    private static final String TABLE_NAME_DOC = "Specify a mapping between Kafka topic name and SingleStore table name";
    private static final String TABLE_NAME_DISPLAY = "SingleStore table name specifying";

    public static  final String FILTER = "singlestore.filter";
    private static final String FILTER_DOC = "Specify a SQL expression to use for filtering incoming data. " +
        "This parameter is inserted directly into the query's WHERE clause and is not SQL-injection safe";
    private static final String FILTER_DISPLAY = "SQL expression to filter incoming data";

    public static  final String TABLE_COLUMN_TO_FIELD = "singlestore.columnToField.<tableName>.<columnName>";
    private static final String TABLE_COLUMN_TO_FIELD_DOC = "Specify a mapping between SingleStoreDB table column names and the Kafka record fields. " +
        "Nested fields are specified as a sequence of field names separated by '.'";
    private static final String TABLE_COLUMN_TO_FIELD_DISPLAY = "Kafka record field for SingleStore column mapping";

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
                    CONNECTION_DATABASE,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    CONNECTION_DATABASE_DOC,
                    CONNECTION_GROUP,
                    2,
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
                    3,
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
                    4,
                    ConfigDef.Width.MEDIUM,
                    CONNECTION_PASSWORD_DISPLAY
            )
            .define(
                    DML_ENDPOINTS,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    DML_ENDPOINTS_DOC,
                    CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.LONG,
                    DML_ENDPOINTS_DISPLAY
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
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.LOW,
                    TABLE_KEY_DOCS,
                    CONNECTION_GROUP,
                    7,
                    ConfigDef.Width.MEDIUM,
                    TABLE_KEY_DISPLAY
            )
            .define(FIELDS_WHITELIST,
                    ConfigDef.Type.LIST,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    FIELDS_WHITELIST_DOCS,
                    DATAMAPPING_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    FIELDS_WHITELIST_DISPLAY
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
            .define(METADATA_TABLE_ALLOW,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    METADATA_TABLE_ALLOW_DOCS,
                    SINGLESTORE_GROUP,
                    1,
                    ConfigDef.Width.MEDIUM,
                    METADATA_TABLE_ALLOW_DISPLAY)
            .define(METADATA_TABLE_NAME,
                    ConfigDef.Type.STRING,
                    "kafka_connect_transaction_metadata",
                    ConfigDef.Importance.LOW,
                    METADATA_TABLE_NAME_DOCS,
                    SINGLESTORE_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    METADATA_TABLE_NAME_DISPLAY,
                    Collections.singletonList(METADATA_TABLE_ALLOW))
            .define(LOAD_DATA_COMPRESSION,
                    ConfigDef.Type.STRING,
                    "GZip",
                    ConfigDef.Importance.LOW,
                    LOAD_DATA_COMPRESSION_DOC,
                    SINGLESTORE_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    LOAD_DATA_COMPRESSION_DISPLAY)
            .define(TABLE_NAME,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.LOW,
                    TABLE_NAME_DOC,
                    SINGLESTORE_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM,
                    TABLE_NAME_DISPLAY)
            .define(FILTER,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                FILTER_DOC,
                SINGLESTORE_GROUP,
                4,
                ConfigDef.Width.MEDIUM,
                FILTER_DISPLAY)
            .define(TABLE_COLUMN_TO_FIELD,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                TABLE_COLUMN_TO_FIELD_DOC,
                SINGLESTORE_GROUP,
                4,
                ConfigDef.Width.MEDIUM,
                TABLE_COLUMN_TO_FIELD_DISPLAY);

    public final String ddlEndpoint;
    public final List<String> dmlEndpoints;
    public final String database;
    public final String user;
    public final String password;
    public final Map<String, String> sqlParams;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final List<TableKey> tableKeys;
    public final DataCompression dataCompression;
    public final boolean metadataTableAllow;
    public final String metadataTableName;
    public final Map<String, String> topicToTableMap;
    public final List<String> fieldsWhitelist;
    public final String filter;
    public final Map<String, List<ColumnMapping>> tableToColumnToFieldMap;


    public SingleStoreSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.ddlEndpoint = getString(DDL_ENDPOINT);
        this.dmlEndpoints = getDmlEndpoints();
        this.database = getString(CONNECTION_DATABASE);
        this.user = getString(CONNECTION_USER);
        this.password = getPasswordValue();
        this.sqlParams = getSqlParams(props);
        this.maxRetries = getInt(MAX_RETRIES);
        this.retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        this.tableKeys = getTableKeys(props);
        this.dataCompression = getDataCompression();
        this.metadataTableAllow = getBoolean(METADATA_TABLE_ALLOW);
        this.metadataTableName = getString(METADATA_TABLE_NAME);
        this.topicToTableMap = getTopicToTableMap(props);
        this.fieldsWhitelist = getList(FIELDS_WHITELIST);
        this.filter = getString(FILTER);
        this.tableToColumnToFieldMap = getTableToColumnToFieldMap(props);

        try {
            JdbcHelper.getDDLConnection(this);
            JdbcHelper.getDMLConnection(this);
        } catch (SQLException ex) {
            throw new ConfigException(ex.getLocalizedMessage());
        }
    }

    private DataCompression getDataCompression() {
        try {
            return DataCompression.valueOf(getString(LOAD_DATA_COMPRESSION).toLowerCase());
        } catch (IllegalArgumentException ex) {
            throw new ConfigException("Configuration \"singlestore.loadDataCompression\" is wrong. Available options: Gzip, LZ4, Skip");
        }
    }

    private Map<String, String> getTopicToTableMap(Map<String, String> props) {
        Map<String, String> topicToTableMap = new HashMap<>();
        String tablePrefix = "singlestore.tableName.";
        props.keySet().stream()
                .filter(key -> key.startsWith(tablePrefix))
                .forEach(key -> topicToTableMap.put(key.substring(tablePrefix.length()), props.get(key)));
        return topicToTableMap;
    }

    private Map<String, List<ColumnMapping>> getTableToColumnToFieldMap(Map<String, String> props) {
        String prefix = "singlestore.columnToField.";
        Map<String, List<ColumnMapping>> map = new HashMap<>();

        props.keySet().stream()
            .filter(key -> key.startsWith(prefix))
            .forEach(key -> {
                String[] tableAndColumn = key.substring(prefix.length()).split("\\.");
                if (tableAndColumn.length != 2) {
                    throw new ConfigException(String.format("Invalid configuration \"%s\"", key));
                }
                String table = tableAndColumn[0];
                String column = tableAndColumn[1];

                if (!map.containsKey(table))
                {
                    map.put(table, new ArrayList<>());
                }

                map.get(table).add(new ColumnMapping(column, props.get(key)));
            });

        return map;
    }

    private Map<String, String> getSqlParams(Map<String, String> props) {
        String paramsPrefix = "params.";
        Map<String, String> sqlParams = new HashMap<>();
        props.keySet().stream()
                .filter(key -> key.startsWith(paramsPrefix))
                .forEach(key -> sqlParams.put(key.substring(paramsPrefix.length()), props.get(key)));
        return sqlParams;
    }

    private String unescapeColumnName(String column) {
        if (column.length() < 2 || !column.startsWith("`") || !column.endsWith("`")) {
            throw new ConfigException(String.format("Configuration \"tableKey\" is wrong. Column name %s is escaped incorrectly", column));
        }

        return column.substring(1, column.length() -1).replace("``", "`");
    }

    private List<String> splitColumnNames(String columns) {
        List<String> res = new ArrayList<>();
        StringBuilder column = new StringBuilder();
        boolean insideOfBackticks = false;
        for (int i = 0; i < columns.length(); i++) {
            if (columns.charAt(i) == '`') {
                insideOfBackticks = !insideOfBackticks;
                column.append(columns.charAt(i));
            } else if (columns.charAt(i) == ',' && !insideOfBackticks) {
                res.add(column.toString());
                column = new StringBuilder();
            } else {
                column.append(columns.charAt(i));
            }
        }
        res.add(column.toString());

        res = res.stream().map(x -> {
            String trimmed = x.trim();
            if (!trimmed.isEmpty() && trimmed.charAt(0) == '`') {
                return unescapeColumnName(trimmed);
            }
            return trimmed;
        }).collect(Collectors.toList());

        return res;
    }

    private List<TableKey> getTableKeys(Map<String, String> props) {
        String tableKeysPrefix = "tableKey.";
        List<TableKey> tableKeys = new ArrayList<>();
        props.keySet().stream()
                .filter(key -> key.startsWith(tableKeysPrefix))
                .forEach(
                        key -> {
                            String[]keyParts = key.split("\\.", 3);
                            if (keyParts.length < 2) {
                                throw new ConfigException(
                                        String.format("Options starting with '%s.' must be formatted correctly. The key should be in the form `%s<index_type>[.<name>]`.", tableKeysPrefix, tableKeysPrefix)
                                );
                            }

                            TableKey.Type keyType;
                            try {
                                keyType = TableKey.Type.valueOf(keyParts[1].toUpperCase());
                            } catch(IllegalArgumentException ex) {
                                throw new ConfigException(
                                        String.format("Option '%s' must specify an index type from the following options: %s", key, Arrays.toString(TableKey.Type.values()))
                                );
                            }

                            String name = "";
                            if (keyParts.length == 3) {
                                name = keyParts[2];
                            }

                            tableKeys.add(new TableKey(keyType, name, splitColumnNames(props.get(key))));
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

    private String getPasswordValue() {
        Password password = getPassword(CONNECTION_PASSWORD);
        if (password != null) {
            return password.value();
        }
        return null;
    }

    public static void main(String... args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
