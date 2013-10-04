/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.indexer;

import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.util.solr.SolrConfigLoader;
import com.ngdata.sep.impl.HBaseShims;
import com.ngdata.sep.util.io.Closer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.ZooKeeper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Instantiates and configures {@code ResultToSolrMapper} instances based on a supplied hbase-indexer configuration.
 */
public class ResultToSolrMapperFactory {

    /**
     * Instantiate a ResultToSolrMapper based on a configuration supplied through an input stream.
     * 
     * @param indexName name of the index for which the mapper is to be created
     * @param indexerConf configuration containing the index definition
     * @param indexConnectionParameters freeform key-value pairs describing the connection information to the outgoing
     *        index
     * @return configured ResultToSolrMapper
     */
    public static ResultToSolrMapper createResultToSolrMapper(String indexName, IndexerConf indexerConf,
            Map<String, String> indexConnectionParameters) {

        ResultToSolrMapper mapper = null;
        try {
            if (indexerConf.getMapperClass() == null) {
                mapper = new DefaultResultToSolrMapper(indexName, indexerConf.getFieldDefinitions(),
                        indexerConf.getDocumentExtractDefinitions(), loadIndexSchema(indexConnectionParameters));
            } else {
                mapper = indexerConf.getMapperClass().newInstance();
                ConfigureUtil.configure(mapper, indexerConf.getGlobalParams());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mapper;

    }

    private static IndexSchema loadIndexSchema(Map<String, String> indexConnectionParameters) throws IOException,
            ParserConfigurationException, SAXException, InterruptedException {
        ZooKeeper zk = new ZooKeeper(indexConnectionParameters.get(SolrConnectionParams.ZOOKEEPER), 30000,
                HBaseShims.getEmptyWatcherInstance());
        SolrConfigLoader solrConfigLoader = new SolrConfigLoader(
                indexConnectionParameters.get(SolrConnectionParams.COLLECTION), zk);

        SolrConfig solrConfig = solrConfigLoader.loadSolrConfig();
        SolrResourceLoader loader = solrConfig.getResourceLoader();
        InputSource is = new InputSource(loader.openSchema("schema.xml"));
        is.setSystemId(SystemIdResolver.createSystemIdFromResourceName("schema.xml"));

        IndexSchema indexSchema = new IndexSchema(solrConfig, "schema.xml", is);
        Closer.close(solrConfigLoader);
        zk.close();
        return indexSchema;

    }

}
