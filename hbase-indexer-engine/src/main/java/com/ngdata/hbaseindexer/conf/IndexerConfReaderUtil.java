package com.ngdata.hbaseindexer.conf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class IndexerConfReaderUtil {

    public static IndexerConfReader getReader(String readerClass) {
        IndexerConfReader reader;
        if (readerClass == null) {
            reader = new XmlIndexerConfReader();
        } else {
            try {
                reader = (IndexerConfReader)Class.forName(readerClass).newInstance();
            } catch (InstantiationException e) {
                throw new AssertionError(e);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        return reader;
    }

    public static IndexerConf getIndexerConf(String readerClass, byte[] configuration) {
            return getReader(readerClass).read(new ByteArrayInputStream(configuration));
    }

    public static IndexerConf getIndexerConf(String readerClass, InputStream configuration) {

        return getReader(readerClass).read(configuration);
    }
}
