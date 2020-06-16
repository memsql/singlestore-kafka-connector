package com.memsql.kafka.utils;

import java.io.OutputStream;

public class DataExtension {

    private String ext;
    private OutputStream outputStream;

    public DataExtension(String ext, OutputStream outputStream) {
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
