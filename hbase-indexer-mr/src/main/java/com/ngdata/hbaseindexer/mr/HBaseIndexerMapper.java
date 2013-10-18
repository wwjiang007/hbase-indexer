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
package com.ngdata.hbaseindexer.mr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

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
import com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    /** Configuration key for setting the HBase table name. */
    public static final String TABLE_NAME_CONF_KEY = "hbase.indexer.table.name";

    private static final String CONF_KEYVALUE_SEPARATOR = "=";

    private static final String CONF_VALUE_SEPARATOR = ";";
    
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndexerMapper.class);
    
    private Indexer indexer;
    private SolrInputDocumentWriter solrDocWriter;

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


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        String indexName = context.getConfiguration().get(INDEX_NAME_CONF_KEY);
        String indexConfiguration = context.getConfiguration().get(INDEX_CONFIGURATION_CONF_KEY);
        String tableName = context.getConfiguration().get(TABLE_NAME_CONF_KEY);
        
        if (indexName == null) {
            throw new IllegalStateException("No configuration value supplied for " + INDEX_NAME_CONF_KEY);
        }
        
        if (indexConfiguration == null) {
            throw new IllegalStateException("No configuration value supplied for " + INDEX_CONFIGURATION_CONF_KEY);
        }
        
        if (tableName == null) {
            throw new IllegalStateException("No configuration value supplied for " + TABLE_NAME_CONF_KEY);
        }

        IndexerConf indexerConf;
        try {
            indexerConf = new XmlIndexerConfReader().read(new ByteArrayInputStream(
                    indexConfiguration.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // TODO This would be better-placed in the top-level job setup -- however, there isn't currently any
        // infrastructure to handle converting an in-memory model into XML (we can only interpret an
        // XML doc into the internal model), so we need to do this here for now
        if (indexerConf.getRowReadMode() != RowReadMode.NEVER) {
            LOG.warn("Changing row read mode from " + indexerConf.getRowReadMode() + " to " + RowReadMode.NEVER);
            indexerConf = new IndexerConfBuilder(indexerConf).rowReadMode(RowReadMode.NEVER).build();
        }
        
        
        Map<String, String> indexConnectionParams = getIndexConnectionParams(context.getConfiguration());

        ResultToSolrMapper mapper = ResultToSolrMapperFactory.createResultToSolrMapper(
                                                    indexName, indexerConf, indexConnectionParams);
        solrDocWriter = createSolrWriter(context, indexConnectionParams);
        indexer = Indexer.createIndexer(indexName, indexerConf, tableName, mapper, null, solrDocWriter);
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
            
            int bufferSize = context.getConfiguration().getInt(SolrOutputFormat.SOLR_RECORD_WRITER_BATCH_SIZE, 100);
            
            return new BufferedSolrInputDocumentWriter(
                    new DirectSolrInputDocumentWriter(conf.get(INDEX_NAME_CONF_KEY), solrServer),
                    bufferSize,
                    context.getCounter(HBaseIndexerCounters.OUTPUT_INDEX_DOCUMENTS));
        } else {
            return new MapReduceSolrInputDocumentWriter(context);
        }
        
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException,
            InterruptedException {
        
        context.getCounter(HBaseIndexerCounters.INPUT_ROWS).increment(1L);
        try {
            indexer.indexRowData(ImmutableList.<RowData>of(new ResultWrappingRowData(result)));
        } catch (SolrServerException e) {
            // These will only be thrown through if there is an exception on the server side.
            // Document-based errors will be swallowed and the counter will be incremented
            throw new RuntimeException(e);
        }

    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            solrDocWriter.close();
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
        
        copyIndexingMetricsToCounters(context);
    }

    private void copyIndexingMetricsToCounters(Context context) {
        final String COUNTER_GROUP = "HBase Indexer Metrics";
        SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics = Metrics.defaultRegistry().groupedMetrics(
                new IndexerMetricsUtil.IndexerMetricPredicate());
        for (Entry<String, SortedMap<MetricName, Metric>> metricsGroupEntry : groupedMetrics.entrySet()) {
            SortedMap<MetricName, Metric> metricsGroupMap = metricsGroupEntry.getValue();
            for (Entry<MetricName, Metric> metricEntry : metricsGroupMap.entrySet()) {
                MetricName metricName = metricEntry.getKey();
                Metric metric = metricEntry.getValue();
                String counterName = metricName.getType() + ": " + metricName.getName();
                if (metric instanceof Counter) {
                    Counter counter = (Counter)metric;
                    context.getCounter(COUNTER_GROUP, counterName).increment(counter.count());
                } else if (metric instanceof Meter) {
                    Meter meter = (Meter)metric;
                    context.getCounter(COUNTER_GROUP, counterName).increment(meter.count());
                } else if (metric instanceof Timer) {
                    Timer timer = (Timer)metric;
                    context.getCounter(COUNTER_GROUP, counterName).increment((long)timer.sum());
                }
            }
        }
    }
    
}
