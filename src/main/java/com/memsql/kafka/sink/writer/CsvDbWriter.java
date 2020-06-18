package com.memsql.kafka.sink.writer;

import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.utils.JdbcHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class CsvDbWriter implements DbWriter {

    private final Schema schema;

    public CsvDbWriter(SinkRecord record) {
        this.schema = record.valueSchema();
    }

    @Override
    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        String columnNames = JdbcHelper.getSchemaTables(schema);
        String queryEnding = String.format("INTO TABLE `%s` (%s)", table, columnNames);
        return String.join(" ", queryPrefix, queryEnding);
    }

    @Override
    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        for (SinkRecord record: records) {
            byte[] value = MemSQLDialect.getRecordValueCSV(record).getBytes(StandardCharsets.UTF_8);
            outputStream.write(value);
            outputStream.write('\n');
        }
    }
}
