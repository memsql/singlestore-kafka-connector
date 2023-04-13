package com.singlestore.kafka.utils;

public class ColumnMapping {
    String columnName;
    String fieldPath;

    public ColumnMapping(String columnName, String fieldPath) {
        this.columnName = columnName;
        this.fieldPath = fieldPath;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getFieldPath() {
        return fieldPath;
    }
}
