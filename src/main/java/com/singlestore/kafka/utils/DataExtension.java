package com.singlestore.kafka.utils;

import java.io.OutputStream;

public class DataExtension {

    private final String ext;
    private final OutputStream outputStream;

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
