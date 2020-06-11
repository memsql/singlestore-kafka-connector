package com.memsql.kafka.sink;

import com.memsql.kafka.utils.DataCompression;
import com.memsql.kafka.utils.JdbcHelper;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
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
            try (com.mysql.jdbc.Connection connection = JdbcHelper.isReferenceTable(config, table)
                    ? (com.mysql.jdbc.Connection)JdbcHelper.getDDLConnection(config)
                    : (com.mysql.jdbc.Connection)JdbcHelper.getDMLConnection(config);
                 com.mysql.jdbc.Statement stmt = (com.mysql.jdbc.Statement) connection.createStatement()) {
                connection.setAllowLoadLocalInfile(true);
                stmt.setLocalInfileInputStream(inputStream);
                DataCompression dataCompression = getDataCompression(config, baseStream);
                OutputStream outputStream = dataCompression.getOutputStream();
            /*
            List<Object> values = records.stream()
                    .map(ConnectRecord::value)
                    .collect(Collectors.toList());
            */
                // TODO Do converting and writing
            }
        } catch (IOException e) {
            // TODO add something here
            e.printStackTrace();
        }

    }

    private DataCompression getDataCompression(MemSQLSinkConfig config, OutputStream baseStream) {
        try {
            switch (config.dataCompression.toLowerCase()) {
                case "gzip" :
                    return new DataCompression("gz", new GZIPOutputStream(baseStream));
                case "lz4" :
                    return new DataCompression("lz4", new LZ4FrameOutputStream(baseStream));
                case "skip" :
                    return new DataCompression("tsv", baseStream);
                default:
                    throw new IllegalArgumentException(String.format("Invalid data compression type. Type %s doesn't exist", config.dataCompression));
            }
        } catch (IOException ex) {
            //TODO change to other exception
            throw new RuntimeException("");
        }
    }

}
