package com.memsql.kafka.sink;

import com.memsql.kafka.sink.writer.AvroDbWriter;
import com.memsql.kafka.sink.writer.CsvDbWriter;
import com.memsql.kafka.sink.writer.DbWriter;
import com.memsql.kafka.utils.DataExtension;
import com.memsql.kafka.utils.DataFormat;
import com.memsql.kafka.utils.JdbcHelper;
import com.mysql.jdbc.Statement;
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
        String table = first.topic();

        JdbcHelper.createTableIfNeeded(config, table, first);
        try (PipedOutputStream baseStream  = new PipedOutputStream();
            InputStream inputStream = new PipedInputStream(baseStream, BUFFER_SIZE)) {
            // TODO think about caching connection instead of opening it each time
            try (Connection connection = JdbcHelper.isReferenceTable(config, table)
                    ? JdbcHelper.getDDLConnection(config)
                    : JdbcHelper.getDMLConnection(config);
                 com.mysql.jdbc.Statement stmt = (com.mysql.jdbc.Statement) connection.createStatement()) {

                if (config.metadataTableAllow) {
                    connection.setAutoCommit(false);
                    String metaId = String.format("%s-%s-%s", first.topic(), first.kafkaPartition(), first.kafkaOffset());
                    Integer recordsCount = records.size();
                    if (JdbcHelper.metadataRecordExists(connection, metaId, config)) {
                        // If metadata record already exists, skip writing this batch of data
                        return;
                    }
                    String metadataQuery = String.format("INSERT INTO `%s` VALUES ('%s', %s)", config.metadataTableName, metaId, recordsCount);
                    stmt.executeUpdate(metadataQuery);
                }

                stmt.setLocalInfileInputStream(inputStream);

                DataExtension dataExtension = getDataCompression(config, baseStream);
                try (OutputStream outputStream = dataExtension.getOutputStream()) {
                    write(config.dataFormat, first, dataExtension, table, outputStream, records, stmt);
                    if (config.metadataTableAllow) {
                        connection.commit();
                    }
                }
            }
        } catch (IOException ex) {
            throw new ConnectException(ex.getLocalizedMessage());
        }
    }

    private void write(DataFormat dataFormat, SinkRecord record, DataExtension dataCompression, String table,
                         OutputStream outputStream, Collection<SinkRecord> records, Statement stmt) throws IOException, SQLException {
        DbWriter dbWriter;
        if (dataFormat == DataFormat.CSV) {
            dbWriter = new CsvDbWriter(record);
        } else {
            dbWriter = new AvroDbWriter(record);
        }
        String dataQuery = dbWriter.generateQuery(dataCompression.getExt(), table);
        dbWriter.writeData(outputStream, records);
        outputStream.close();
        stmt.executeUpdate(dataQuery);
    }

    private DataExtension getDataCompression(MemSQLSinkConfig config, OutputStream baseStream) {
        try {
            switch (config.dataCompression) {
                case gzip:
                    return new DataExtension("gz", new GZIPOutputStream(baseStream));
                case lz4:
                    return new DataExtension("lz4", new LZ4FrameOutputStream(baseStream));
                case skip:
                    return new DataExtension("tsv", baseStream);
                default:
                    throw new IllegalArgumentException(String.format("Invalid data compression type. Type `%s` doesn't exist", config.dataCompression));
            }
        } catch (IOException ex) {
            throw new ConnectException(ex.getLocalizedMessage());
        }
    }

}
