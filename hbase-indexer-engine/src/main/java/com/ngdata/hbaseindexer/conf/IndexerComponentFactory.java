package com.ngdata.hbaseindexer.conf;

import java.io.InputStream;
import java.util.Map;

import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;

public interface IndexerComponentFactory {
    void configure(InputStream is) throws IndexerConfException;

    IndexerConf createIndexerConf() throws IndexerConfException;

    ResultToSolrMapper createMapper(String indexerConf, Map<String, String> connectionParams) throws IndexerConfException;

}
