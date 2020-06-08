package com.memsql.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class MemSQLSinkConfig extends AbstractConfig {

    private static final String CONNECTION_GROUP = "Connection";

    public static final String DDL_ENDPOINT = "memsql.ddlEndpoint";
    private static final String DDL_ENDPOINT_DOC =
            "Hostname or IP address of the MemSQL Master Aggregator in the format `host[:port]`";
    private static final String DDL_ENDPOINT_DISPLAY = "DDL Endpoint";

    public static final String MEMSQL_USER = "memsql.user";
    private static final String MEMSQL_USER_DOC = "MemSQL connection user.";
    private static final String MEMSQL_USER_DISPLAY = "MemSQL User";

    public static final String MEMSQL_PASSWORD = "memsql.password";
    private static final String MEMSQL_PASSWORD_DOC = "MemSQL connection password.";
    private static final String MEMSQL_PASSWORD_DISPLAY = "MemSQL Password";

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
                    MEMSQL_USER,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.HIGH,
                    MEMSQL_USER_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.MEDIUM,
                    MEMSQL_USER_DISPLAY
            )
            .define(
                    MEMSQL_PASSWORD,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.HIGH,
                    MEMSQL_PASSWORD_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.MEDIUM,
                    MEMSQL_PASSWORD_DISPLAY
            );

    public final String ddlEndpoint;
    public final String user;
    public final String password;

    public MemSQLSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        this.ddlEndpoint = getString(DDL_ENDPOINT);
        this.user = getString(MEMSQL_USER);
        this.password = getPasswordValue(MEMSQL_PASSWORD);
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
