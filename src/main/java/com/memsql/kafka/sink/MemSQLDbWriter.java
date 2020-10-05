package com.memsql.kafka.sink;

import com.memsql.kafka.utils.DataExtension;
import com.memsql.kafka.utils.JdbcHelper;

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
import java.util.Collection;
import java.util.zip.GZIPOutputStream;

public class MemSQLDbWriter {

    private static final Logger log = LoggerFactory.getLogger(MemSQLDbWriter.class);
    private final MemSQLSinkConfig config;

    private final int BUFFER_SIZE = 524288;

    public MemSQLDbWriter(MemSQLSinkConfig config) {
        this.config = config;
    }

    public void write(Collection<SinkRecord> records) throws SQLException {
        SinkRecord first = records.iterator().next();
        String table = JdbcHelper.getTableName(first.topic(), config);

        JdbcHelper.createTableIfNeeded(config, table, first.valueSchema());
        try (PipedOutputStream baseStream  = new PipedOutputStream();
            InputStream inputStream = new PipedInputStream(baseStream, BUFFER_SIZE)) {
            // TODO think about caching connection instead of opening it each time
            try (Connection connection = JdbcHelper.isReferenceTable(config, table)
                    ? JdbcHelper.getDDLConnection(config)
                    : JdbcHelper.getDMLConnection(config);
                 Statement stmt = connection.createStatement()) {

                if (config.metadataTableAllow) {
                    String metaId = String.format("%s-%s-%s", first.topic(), first.kafkaPartition(), first.kafkaOffset());
                    if (JdbcHelper.metadataRecordExists(connection, metaId, config)) {
                        // If metadata record already exists, skip writing this batch of data
                        return;
                    }
                    connection.setAutoCommit(false);
                    Integer recordsCount = records.size();
                    try (PreparedStatement metadataStmt = MemSQLDialect.getInsertIntoMetadataQuery(connection, config.metadataTableName, metaId, recordsCount)) {
                        log.trace("Executing SQL:\n{}", metadataStmt);
                        metadataStmt.executeUpdate();
                    }
                }

                ((org.mariadb.jdbc.MariaDbStatement)stmt).setLocalInfileInputStream(inputStream);

                DataExtension dataExtension = getDataExtension(baseStream);
                try (OutputStream outputStream = dataExtension.getOutputStream()) {
                    write(first, dataExtension, table, outputStream, records, stmt);
                    if (config.metadataTableAllow) {
                        connection.commit();
                    }
                }
            }
        } catch (IOException ex) {
            throw new ConnectException(ex.getLocalizedMessage());
        }
    }

    private void write(SinkRecord record, DataExtension dataCompression, String table,
                         OutputStream outputStream, Collection<SinkRecord> records, Statement stmt) throws IOException, SQLException {
        CsvDbWriter dbWriter = new CsvDbWriter(record);
        String dataQuery = dbWriter.generateQuery(dataCompression.getExt(), table);
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
