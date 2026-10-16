package com.singlestore.kafka.sink;

import com.singlestore.kafka.utils.DataExtension;
import com.singlestore.kafka.utils.DataTransform;
import com.singlestore.kafka.utils.JdbcHelper;

import java.sql.PreparedStatement;
import java.sql.Statement;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class SingleStoreDbWriter {

    private static final Logger log = LoggerFactory.getLogger(SingleStoreDbWriter.class);
    private final SingleStoreSinkConfig config;

    private final int BUFFER_SIZE = 524288;

    public SingleStoreDbWriter(SingleStoreSinkConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> rawRecords) throws SQLException {
        Collection<SinkRecord> records = new DataTransform(config.fieldsWhitelist, config.fieldsBlacklist)
                .selectFields(rawRecords);
        Map<String, Collection<SinkRecord>> tableToRecords = new HashMap<>();
        SinkRecord first = records.iterator().next();

        if (config.recordToTableMappingField == null) {
            String table = JdbcHelper.getTableName(first, config);
            tableToRecords.put(table, records);
        } else {
            for (SinkRecord record: records) {
                String table = JdbcHelper.getTableName(record, config);
                if (table == null) {
                    continue;
                }

                if (!tableToRecords.containsKey(table)) {
                    tableToRecords.put(table, new ArrayList<>());
                }
                tableToRecords.get(table).add(record);
            }
        }

        for (Map.Entry<String, Collection<SinkRecord>> entry: tableToRecords.entrySet()) {
            String table = entry.getKey();
            SinkRecord record = entry.getValue().iterator().next();
            JdbcHelper.createTableIfNeeded(config, table, record.valueSchema());
        }

        boolean writeToReferenceTable = false;
        for (String table: tableToRecords.keySet()) {
            if (JdbcHelper.isReferenceTable(config, table)) {
                writeToReferenceTable = true;
                break;
            }
        }

        // TODO think about caching connection instead of opening it each time
        try (Connection connection = writeToReferenceTable
            ? JdbcHelper.getDDLConnection(config)
            : JdbcHelper.getDMLConnection(config);
             Statement stmt = connection.createStatement()) {
            String metaId = String.format("%s-%s-%s", first.topic(), first.kafkaPartition(), first.kafkaOffset());
            if (config.metadataTableAllow && JdbcHelper.metadataRecordExists(connection, metaId, config)) {
                // If metadata record already exists, skip writing this batch of data
                return;
            }

            Map<String, InsertDbWriter> insertWriters = new HashMap<>();
            if (config.onDuplicateKeySQL != null) {
                for (Map.Entry<String, Collection<SinkRecord>> entry: tableToRecords.entrySet()) {
                    String table = entry.getKey();
                    Collection<SinkRecord> tableRecords = entry.getValue();
                    SinkRecord firstTableRecord = tableRecords.iterator().next();
                    InsertDbWriter insertWriter = new InsertDbWriter(config, firstTableRecord, table);
                    insertWriters.put(table, insertWriter);

                    String createTemporaryTable = insertWriter.generateCreateTemporaryTableQuery();
                    log.trace("Executing SQL:\n{}", createTemporaryTable);
                    stmt.executeUpdate(createTemporaryTable);

                    CsvDbWriter csvWriter = new CsvDbWriter(
                            config, firstTableRecord, insertWriter.getTemporaryTable(), table,
                            null, false, insertWriter.getOrderColumn(), insertWriter.getStagingColumns());
                    writeWithLoadData(stmt, csvWriter, tableRecords);
                }
            }

            if (config.metadataTableAllow) {
                connection.setAutoCommit(false);
                Integer recordsCount = records.size();
                try (PreparedStatement metadataStmt = SingleStoreDialect.getInsertIntoMetadataQuery(connection, config.metadataTableName, metaId, recordsCount)) {
                    log.trace("Executing SQL:\n{}", metadataStmt);
                    metadataStmt.executeUpdate();
                }
            }

            if (config.onDuplicateKeySQL != null) {
                for (InsertDbWriter insertWriter: insertWriters.values()) {
                    String insert = insertWriter.generateInsertQuery();
                    log.trace("Executing SQL:\n{}", insert);
                    stmt.executeUpdate(insert);
                }
            } else {
                // TODO: investigate parallelization of this loop
                for (Map.Entry<String, Collection<SinkRecord>> entry: tableToRecords.entrySet()) {
                    SinkRecord firstTableRecord = entry.getValue().iterator().next();
                    writeWithLoadData(stmt, new CsvDbWriter(config, firstTableRecord, entry.getKey()), entry.getValue());
                }
            }

            if (config.metadataTableAllow) {
                connection.commit();
            }
        } catch (IOException ex) {
            throw new ConnectException(ex.getLocalizedMessage());
        }
    }

    private void writeWithLoadData(Statement stmt, CsvDbWriter dbWriter,
                                   Collection<SinkRecord> tableRecords) throws IOException, SQLException {
        try (PipedOutputStream baseStream  = new PipedOutputStream();
             InputStream inputStream = new PipedInputStream(baseStream, BUFFER_SIZE)) {
            ((com.singlestore.jdbc.Statement)stmt).setNextLocalInfileInputStream(inputStream);

            DataExtension dataExtension = getDataExtension(baseStream);
            try (OutputStream outputStream = dataExtension.getOutputStream()) {
                write(dbWriter, dataExtension, outputStream, tableRecords, stmt);
            }
        }
    }

    private void write(CsvDbWriter dbWriter, DataExtension dataCompression,
                         OutputStream outputStream, Collection<SinkRecord> records, Statement stmt) throws IOException, SQLException {
        String dataQuery = dbWriter.generateQuery(dataCompression.getExt());
        dbWriter.writeData(outputStream, records);
        outputStream.close();
        log.trace("Executing SQL:\n{}", dataQuery);
        stmt.executeUpdate(dataQuery);
    }

    private DataExtension getDataExtension(OutputStream baseStream) {
        try {
            switch (this.config.dataCompression) {
                case gzip:
                    return new DataExtension("gz", new GZIPOutputStream(baseStream));
                case lz4:
                    return new DataExtension("lz4", new LZ4FrameOutputStream(baseStream));
                case skip:
                    return new DataExtension("tsv", baseStream);
                default:
                    throw new ConnectException(String.format("Invalid data compression type. Type `%s` doesn't exist", config.dataCompression));
            }
        } catch (IOException ex) {
            throw new ConnectException(ex.getLocalizedMessage());
        }
    }
}
