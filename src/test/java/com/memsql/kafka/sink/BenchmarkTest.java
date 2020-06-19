package com.memsql.kafka.sink;

import com.memsql.kafka.utils.SinkRecordCreator;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.*;

public class BenchmarkTest {

    @Ignore
    @Test
    public void benchmarkTest() throws Exception {
        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:3306");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};

        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);

        List<SinkRecord> records = SinkRecordCreator.createRecords(1000000);

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "avro");
        long startTimeAvro = System.nanoTime();
        writer.write(records);
        System.out.println("Avro time: " + (System.nanoTime() - startTimeAvro));

        props.put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "csv");
        long startTimeCsv = System.nanoTime();
        writer.write(records);
        System.out.println("CSV time:  " + (System.nanoTime() - startTimeCsv));
    }

    @Ignore
    @Test
    public void dataCompressionBenchmarkTest() throws Exception {

        int iterations = 100;
        int recordCount = 5000;

        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:3306");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);
        List<SinkRecord> records = SinkRecordCreator.createRecords(recordCount);

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "skip");
        write(iterations, writer, records, "Skip time: ");

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "gzip");
        write(iterations, writer, records, "Gzip time: ");

        props.put(MemSQLSinkConfig.LOAD_DATA_COMPRESSION, "lz4");
        write(iterations, writer, records, "LZ4 time:  ");
    }

    @Ignore
    @Test
    public void avroTest() throws Exception {
        Map<String, String> props = new HashMap<String, String>() {{
            put(MemSQLSinkConfig.DDL_ENDPOINT, "localhost:3306");
            put(MemSQLSinkConfig.CONNECTION_DATABASE, "testdb");
            put(MemSQLSinkConfig.CONNECTION_USER, "root");
            put(MemSQLSinkConfig.METADATA_TABLE_ALLOW, "false");
            put(MemSQLSinkConfig.LOAD_DATA_FORMAT, "Avro");
        }};
        MemSQLSinkConfig config = new MemSQLSinkConfig(props);
        MemSQLDbWriter writer = new MemSQLDbWriter(config);
        Schema schema = SchemaBuilder.array(
                Schema.STRING_SCHEMA
        );
        List<String> values = new ArrayList<String>() {{
            add("Name");
            add("Age");
        }};
        List<SinkRecord> records = Collections.nCopies(100, SinkRecordCreator.createRecord(schema, values, "topicArray"));
        writer.write(records);
    }

    private void write(int n, MemSQLDbWriter writer, List<SinkRecord> records, String message) throws SQLException {
        long startTime = System.nanoTime();
        for (int i = 0; i < n; i++) {
            writer.write(records);
        }
        System.out.println(message + (System.nanoTime() - startTime));
    }
}
