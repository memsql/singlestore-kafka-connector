package com.memsql.kafka.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

public interface DbWriter {

    String generateQuery(String ext, String table);

    void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException;
}
