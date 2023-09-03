package com.getindata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JSR310Module;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestDataFormat extends SimpleStreamFormat<TestData> {

    private static final long serialVersionUID = 1L;
    public static final String DEFAULT_CHARSET_NAME = "UTF-8";


    @Override
    public Reader createReader(Configuration configuration, FSDataInputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, DEFAULT_CHARSET_NAME));
        return new Reader(reader);
    }

    @Override
    public TypeInformation<TestData> getProducedType() {
        return TypeInformation.of(TestData.class);
    }

    public static final class Reader implements StreamFormat.Reader<TestData> {
        private final BufferedReader reader;
        private final ObjectMapper mapper;

        Reader(BufferedReader reader) {
            this.reader = reader;
            this.mapper = new ObjectMapper();
            this.mapper.registerModule(new JSR310Module());
        }

        @Nullable
        public TestData read() throws IOException {
            String raw = this.reader.readLine();
            if (raw == null) {
                return null;
            }
            return this.mapper.readValue(raw, TestData.class);
        }

        public void close() throws IOException {
            this.reader.close();
        }
    }
}
