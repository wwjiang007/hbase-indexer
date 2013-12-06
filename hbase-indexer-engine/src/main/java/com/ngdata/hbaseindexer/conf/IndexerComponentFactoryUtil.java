package com.ngdata.hbaseindexer.conf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class IndexerComponentFactoryUtil {

    public static IndexerComponentFactory getComponentFactory(String factoryClass, InputStream configuration) {
        IndexerComponentFactory factory;
        if (factoryClass == null) {
            factory = new DefaultIndexerComponentFactory();
        } else {
            try {
                factory = (IndexerComponentFactory)Class.forName(factoryClass).newInstance();
            } catch (InstantiationException e) {
                throw new AssertionError(e);
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        factory.configure(configuration);
        return factory;
    }

}
