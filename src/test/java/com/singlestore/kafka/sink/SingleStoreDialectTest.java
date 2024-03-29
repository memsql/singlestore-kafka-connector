package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.ColumnMapping;
import com.singlestore.kafka.utils.TableKey;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

public class SingleStoreDialectTest {
    @Test
    public void getSchemaForCrateTableQueryStruct() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.STRING_SCHEMA)
                .field("f3", Schema.STRING_SCHEMA)
                .field("f4", Schema.STRING_SCHEMA)
                .field("f5", Schema.STRING_SCHEMA)
                .field("f6", Schema.STRING_SCHEMA)
                .build();

        List<TableKey> keys = new ArrayList<>(Arrays.asList(
                new TableKey(TableKey.Type.COLUMNSTORE, "n1", Collections.singletonList("f1")),
                new TableKey(TableKey.Type.UNIQUE, "n2", new ArrayList<>(Arrays.asList("f2", "f1"))),
                new TableKey(TableKey.Type.PRIMARY, "n3", Collections.singletonList("f3")),
                new TableKey(TableKey.Type.SHARD, "n4", Collections.singletonList("f4")),
                new TableKey(TableKey.Type.KEY, "", Collections.singletonList("f5"))
                ));

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, null), "(\n" +
                "`f1` TEXT NOT NULL,\n" +
                "`f2` TEXT NOT NULL,\n" +
                "`f3` TEXT NOT NULL,\n" +
                "`f4` TEXT NOT NULL,\n" +
                "`f5` TEXT NOT NULL,\n" +
                "`f6` TEXT NOT NULL,\n" +
                "KEY `n1`(`f1`) USING CLUSTERED COLUMNSTORE,\n" +
                "UNIQUE KEY `n2`(`f2`, `f1`),\n" +
                "PRIMARY KEY `n3`(`f3`),\n" +
                "SHARD KEY `n4`(`f4`),\n" +
                "KEY (`f5`)\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableColumnstore() throws SQLException {
        Schema schema = SchemaBuilder.struct()
                .field("f1", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .field("f2", Schema.STRING_SCHEMA)
                .build();

        List<TableKey> keys = new ArrayList<>(Collections.emptyList());

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, null), "(\n" +
                "`f1` JSON NOT NULL,\n" +
                "`f2` TEXT NOT NULL,\n" +
                "KEY (`f2`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }


    @Test
    public void getSchemaForCrateTableQueryNotStruct() throws SQLException {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>(Collections.singletonList(
                new TableKey(TableKey.Type.COLUMNSTORE, "", Collections.singletonList("data"))
        ));

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, null), "(\n" +
                "`data` TEXT NOT NULL,\n" +
                "KEY (`data`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getSchemaForCrateTableQueryNoKeys() throws SQLException {
        Schema schema = Schema.STRING_SCHEMA;

        List<TableKey> keys = new ArrayList<>();

        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, null), "(\n" +
                "`data` TEXT NOT NULL,\n" +
                "KEY (`data`) USING CLUSTERED COLUMNSTORE\n" +
                ")");
    }

    @Test
    public void getSchemaForCreateTableQueryColumnMapping() throws SQLException {
        Schema schema = SchemaBuilder.struct()
            .field("f1", Schema.STRING_SCHEMA)
            .field("f2", Schema.INT64_SCHEMA)
            .field("f3",
                SchemaBuilder.struct().field("c1", Schema.FLOAT64_SCHEMA))
            .build();

        List<TableKey> keys = new ArrayList<>();
        List<ColumnMapping> mappings = Arrays.asList(
            new ColumnMapping("c1", "f1"),
            new ColumnMapping("c2", "f3.c1")
        );


        assertEquals(SingleStoreDialect.getSchemaForCreateTableQuery(schema, keys, mappings), "(\n" +
            "`c1` TEXT NOT NULL,\n" +
            "`c2` DOUBLE NOT NULL,\n" +
            "KEY (`c1`) USING CLUSTERED COLUMNSTORE\n" +
            ")");
    }

    @Test
    public void escapeColumnNames() {
        assertEquals(SingleStoreDialect.escapeColumnNames(Arrays.asList("qwe-rty", "`\\\\")), "`qwe-rty`, ```\\\\`");
        assertEquals(SingleStoreDialect.escapeColumnNames(Collections.singletonList("qwe-rty")), "`qwe-rty`");
        assertEquals(SingleStoreDialect.escapeColumnNames(Collections.emptyList()), "");
    }
}
