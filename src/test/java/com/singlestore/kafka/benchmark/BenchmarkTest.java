package com.singlestore.kafka.benchmark;

import com.singlestore.kafka.sink.SingleStoreDbWriter;
import com.singlestore.kafka.sink.SingleStoreSinkConfig;
import com.singlestore.kafka.utils.SinkRecordCreator;
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
            put(SingleStoreSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testdb");
            put(SingleStoreSinkConfig.CONNECTION_USER, "root");
            put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};

        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        SingleStoreDbWriter writer = new SingleStoreDbWriter(config);

        int numberOfRecords = 1000000;
        List<SinkRecord> records = SinkRecordCreator.createRecords(numberOfRecords);

        long startTime = System.nanoTime();
        writer.write(records);
        System.out.println(String.format("Time to write %s records: %s ns", numberOfRecords, (System.nanoTime() - startTime)));
    }

    @Ignore
    @Test
    public void dataCompressionBenchmarkTest() throws Exception {

        int iterations = 100;
        int recordCount = 5000;

        Map<String, String> props = new HashMap<String, String>() {{
            put(SingleStoreSinkConfig.DDL_ENDPOINT, "localhost:5506");
            put(SingleStoreSinkConfig.CONNECTION_DATABASE, "testdb");
            put(SingleStoreSinkConfig.CONNECTION_USER, "root");
            put(SingleStoreSinkConfig.METADATA_TABLE_ALLOW, "false");
        }};
        SingleStoreSinkConfig config = new SingleStoreSinkConfig(props);
        SingleStoreDbWriter writer = new SingleStoreDbWriter(config);
        List<SinkRecord> records = SinkRecordCreator.createRecords(recordCount);

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "skip");
        write(iterations, writer, records, "Skip time: ");

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "gzip");
        write(iterations, writer, records, "Gzip time: ");

        props.put(SingleStoreSinkConfig.LOAD_DATA_COMPRESSION, "lz4");
        write(iterations, writer, records, "LZ4 time:  ");
    }

    private void write(int n, SingleStoreDbWriter writer, List<SinkRecord> records, String message) throws SQLException {
        long startTime = System.nanoTime();
        for (int i = 0; i < n; i++) {
            writer.write(records);
        }
        System.out.println(message + (System.nanoTime() - startTime));
    }
}
