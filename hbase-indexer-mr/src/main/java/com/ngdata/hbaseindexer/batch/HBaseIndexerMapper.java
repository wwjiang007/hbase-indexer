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
package com.ngdata.hbaseindexer.batch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfReader;
import com.ngdata.hbaseindexer.indexer.DirectSolrInputDocumentWriter;
import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.indexer.ResultToSolrMapperFactory;
import com.ngdata.hbaseindexer.indexer.ResultWrappingRowData;
import com.ngdata.hbaseindexer.indexer.RowData;
import com.ngdata.hbaseindexer.indexer.SolrInputDocumentWriter;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.hadoop.SolrInputDocumentWritable;

/**
 * Mapper for converting HBase Result objects into index documents.
 */
public class HBaseIndexerMapper extends TableMapper<Text, SolrInputDocumentWritable> {

    /** Configuration key for setting the name of the indexer. */
    public static final String INDEX_NAME_CONF_KEY = "hbase.indexer.indexname";

    /** Configuration key for setting the contents of the indexer config. */
    public static final String INDEX_CONFIGURATION_CONF_KEY = "hbase.indexer.configuration";
    
    /** Configuration key for setting the free-form index connection parameters. */
    public static final String INDEX_CONNECTION_PARAMS_CONF_KEY = "hbase.indexer.index.connectionparams";
    
    /** Configuration key for setting the direct write flag. */
    public static final String INDEX_DIRECT_WRITE_CONF_KEY = "hbase.indexer.directwrite";

    private static final String CONF_KEYVALUE_SEPARATOR = "=";

    private static final String CONF_VALUE_SEPARATOR = ";";
    
    private static final Log LOG = LogFactory.getLog(HBaseIndexerMapper.class);

    /**
     * Add the given index connection parameters to a Configuration.
     * 
     * @param conf the configuration in which to add the parameters
     * @param connectionParams index connection parameters
     */
    public static void configureIndexConnectionParams(Configuration conf, Map<String, String> connectionParams) {
        String confValue = Joiner.on(CONF_VALUE_SEPARATOR).withKeyValueSeparator(CONF_KEYVALUE_SEPARATOR).join(
                connectionParams);

        conf.set(INDEX_CONNECTION_PARAMS_CONF_KEY, confValue);
    }

    /**
     * Retrieve index connection parameters from a Configuration.
     * 
     * @param conf configuration containing index connection parameters
     * @return index connection parameters
     */
    public static Map<String, String> getIndexConnectionParams(Configuration conf) {
        String confValue = conf.get(INDEX_CONNECTION_PARAMS_CONF_KEY);
        if (confValue == null) {
            LOG.warn("No connection parameters found in configuration");
            return ImmutableMap.of();
        }

        return Splitter.on(CONF_VALUE_SEPARATOR).withKeyValueSeparator(CONF_KEYVALUE_SEPARATOR).split(confValue);
    }

    private Indexer indexer;
  

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        String indexName = context.getConfiguration().get(INDEX_NAME_CONF_KEY);
        // TODO Load the configuration from the distributed cache instead of putting it in the Configuration everywhere
        String indexConfiguration = context.getConfiguration().get(INDEX_CONFIGURATION_CONF_KEY);
        
        if (indexName == null) {
            throw new IllegalStateException("No configuration value supplied for " + INDEX_NAME_CONF_KEY);
        }
        
        if (indexConfiguration == null) {
            throw new IllegalStateException("No configuration value supplied for " + INDEX_CONFIGURATION_CONF_KEY);
        }

        IndexerConf indexerConf;
        try {
            indexerConf = new XmlIndexerConfReader().read(new ByteArrayInputStream(
                    indexConfiguration.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // TODO Move this to the top-level job setup
        if (indexerConf.getRowReadMode() != RowReadMode.NEVER) {
            LOG.warn("Changing row read mode from " + indexerConf.getRowReadMode() + " to " + RowReadMode.NEVER);
            indexerConf = new IndexerConfBuilder(indexerConf).rowReadMode(RowReadMode.NEVER).build();
        }
        
        
        Map<String, String> indexConnectionParams = getIndexConnectionParams(context.getConfiguration());

        ResultToSolrMapper mapper = ResultToSolrMapperFactory.createResultToSolrMapper(
                                                    indexName, indexerConf, indexConnectionParams);
        SolrInputDocumentWriter solrDocWriter = createSolrWriter(context, indexConnectionParams);
        indexer = Indexer.createIndexer(indexName, indexerConf, mapper, null, solrDocWriter);
    }
    
    private SolrInputDocumentWriter createSolrWriter(Context context, Map<String,String> indexConnectionParams) throws IOException {
        Configuration conf = context.getConfiguration();
        if (conf.getBoolean(INDEX_DIRECT_WRITE_CONF_KEY, false)) {
            String indexZkHost = indexConnectionParams.get(SolrConnectionParams.ZOOKEEPER);
            String collectionName = indexConnectionParams.get(SolrConnectionParams.COLLECTION);
            
            if (indexZkHost == null) {
                throw new IllegalStateException("No index ZK host defined");
            }
            
            if (collectionName == null) {
                throw new IllegalStateException("No collection name defined");
            }
            CloudSolrServer solrServer = new CloudSolrServer(indexZkHost);
            solrServer.setDefaultCollection(collectionName);
            return new DirectSolrInputDocumentWriter(conf.get(INDEX_NAME_CONF_KEY), solrServer);
        } else {
            return new MapReduceSolrInputDocumentWriter(context);
        }
        
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException,
            InterruptedException {
        
        try {
            indexer.indexRowData(ImmutableList.<RowData>of(new ResultWrappingRowData(result)));
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }

    }

}
