package com.ngdata.hbaseindexer.conf;

import java.io.InputStream;

public interface IndexerConfReader {
    IndexerConf read(InputStream is) throws IndexerConfException;

    void validate(InputStream is) throws IndexerConfException;
}
