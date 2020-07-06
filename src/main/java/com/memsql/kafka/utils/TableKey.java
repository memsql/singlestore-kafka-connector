package com.memsql.kafka.utils;

import com.memsql.kafka.sink.MemSQLDialect;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.stream.Collectors;

public class TableKey {
    public enum Type {
        PRIMARY,
        COLUMNSTORE,
        UNIQUE,
        SHARD,
        KEY
    }

    private Type type;
    private String name;
    private List<String> columns;

    public TableKey(Type t, String n, List<String> c) {
        type = t;
        name = n;
        columns = c;
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        String columnsSql = "(" + this.columns.stream().map(MemSQLDialect::quoteIdentifier).collect(Collectors.joining(", ")) + ")";
        String nameSql = this.name == null || this.name.isEmpty() ? "" : MemSQLDialect.quoteIdentifier(this.name);

        switch(this.type) {
            case PRIMARY:
                return String.format("PRIMARY KEY %s%s", nameSql, columnsSql);
            case COLUMNSTORE:
                return String.format("KEY %s%s USING CLUSTERED COLUMNSTORE", nameSql, columnsSql);
            case UNIQUE:
                return String.format("UNIQUE KEY %s%s", nameSql, columnsSql);
            case SHARD:
                return String.format("SHARD KEY %s%s", nameSql, columnsSql);
            case KEY:
                return String.format("KEY %s%s", nameSql, columnsSql);
            default:
                throw new ConnectException(String.format("unsupported key type: %s", type));
        }
    }
}
