package com.memsql.kafka.sink.writer;

import com.memsql.kafka.sink.MemSQLDialect;
import com.memsql.kafka.utils.AvroSchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AvroDbWriter implements DbWriter {

    private final Schema avroSchema;

    public AvroDbWriter(SinkRecord record) {
        this.avroSchema = AvroSchemaConverter.toAvroType(record.valueSchema());
    }

    @Override
    public String generateQuery(String ext, String table) {
        String queryPrefix = String.format("LOAD DATA LOCAL INFILE '###.%s'", ext);
        List<Schema> nullableSchemas = new ArrayList<>();
        avroSchema.getFields().forEach(field ->
                nullableSchemas.add(AvroSchemaConverter.resolveNullableType(field.schema(), field.schema().isNullable()))
        );
        List<String> avroSchemaParts = new ArrayList<>();
        int fieldsSize = avroSchema.getFields().size();
        for (int i = 0; i < fieldsSize; i++) {
            Schema.Field currentField = avroSchema.getFields().get(i);
            String avroSchemaMapping = MemSQLDialect.quoteIdentifier(currentField.name()) +  " <- %::" + currentField.name();
            String avroSchemaPart = currentField.schema().isNullable() ?
                    String.format("%s::%s", avroSchemaMapping, nullableSchemas.get(i).getType().getName()) : avroSchemaMapping;
            avroSchemaParts.add(avroSchemaPart);
        }

        String avroMapping = "( " + String.join(", ", avroSchemaParts) + " )";
        String queryEnding = String.format("INTO TABLE `%s` FORMAT AVRO %s SCHEMA '%s'",
                table, avroMapping, avroSchema.toString());
        return String.join(" ", queryPrefix, queryEnding);
    }

    @Override
    public void writeData(OutputStream outputStream, Collection<SinkRecord> records) throws IOException {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericData.Record avroRecord = new GenericData.Record(avroSchema);
        for (SinkRecord r: records) {
            List<Object> value = MemSQLDialect.getRecordValues(r);
            for (int i = 0; i < value.size(); i++) {
                avroRecord.put(i, value.get(i));
            }
            datumWriter.write(avroRecord, encoder);
        }
        encoder.flush();
    }
}
