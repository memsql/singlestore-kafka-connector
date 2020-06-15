package com.memsql.kafka.utils;

import java.io.OutputStream;

public class DataCompression {

    private String ext;
    private OutputStream outputStream;

    public DataCompression(String ext, OutputStream outputStream) {
        this.ext = ext;
        this.outputStream = outputStream;
    }

    public String getExt() {
        return ext;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }
}
