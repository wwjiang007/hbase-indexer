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

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;
import static com.ngdata.sep.impl.HBaseShims.newResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration, and eventually
 * calls Solr.
 */
public abstract class Indexer {

    protected Log log = LogFactory.getLog(getClass());

    private String indexerName;
    protected IndexerConf conf;
    private String tableName;
    private SolrInputDocumentWriter solrWriter;
    protected ResultToSolrMapper mapper;
    protected UniqueKeyFormatter uniqueKeyFormatter;
    


    /**
     * Instantiate an indexer based on the given {@link IndexerConf}.
     */
    public static Indexer createIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper, HTablePool tablePool,
            SolrInputDocumentWriter solrWriter) {
        switch (conf.getMappingType()) {
        case COLUMN:
            return new ColumnBasedIndexer(indexerName, conf, tableName, mapper, solrWriter);
        case ROW:
            return new RowBasedIndexer(indexerName, conf, tableName, mapper, tablePool, solrWriter);
        default:
            throw new IllegalStateException("Can't determine the type of indexing to use for mapping type "
                    + conf.getMappingType());
        }
    }

    Indexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper, SolrInputDocumentWriter solrWriter) {
        this.indexerName = indexerName;
        this.conf = conf;
        this.tableName = tableName;
        this.mapper = mapper;
        try {
            this.uniqueKeyFormatter = conf.getUniqueKeyFormatterClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
        ConfigureUtil.configure(uniqueKeyFormatter, conf.getGlobalParams());
        this.solrWriter = solrWriter;
       
    }
    
    /**
     * Returns the name of this indexer.
     * 
     * @return indexer name
     */
    public String getName() {
        return indexerName;
    }
    

    /**
     * Build all new documents and ids to delete based on a list of {@code RowData}s.
     * 
     * @param rowDataList list of RowData instances to be considered for indexing
     * @param updateCollector collects updates to be written to Solr
     */
    abstract void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException;
    

    /**
     * Create index documents based on a nested list of RowData instances.
     * 
     * @param rowDataList list of RowData instances to be considered for indexing
     */
    public void indexRowData(List<RowData> rowDataList) throws IOException, SolrServerException {
        SolrUpdateCollector updateCollector = new SolrUpdateCollector(rowDataList.size());
        calculateIndexUpdates(rowDataList, updateCollector);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Indexer %s will send to Solr %s adds and %s deletes", getName(),
                    updateCollector.getDocumentsToAdd().size(), updateCollector.getIdsToDelete().size()));
        }

        if (!updateCollector.getDocumentsToAdd().isEmpty()) {
            solrWriter.add(updateCollector.getDocumentsToAdd());
        }
        if (!updateCollector.getIdsToDelete().isEmpty()) {
            solrWriter.deleteById(updateCollector.getIdsToDelete());
        }
        
        for (String deleteQuery : updateCollector.getDeleteQueries()) {
            solrWriter.deleteByQuery(deleteQuery);
        }
        
    }

    
    public void stop() {
        IndexerMetricsUtil.shutdownMetrics(getClass(), indexerName);
        IndexerMetricsUtil.shutdownMetrics(mapper.getClass(), indexerName);
    }
    
    void addTableName(SolrInputDocument document) {
        if (conf.getTableNameField() != null) {
            document.addField(conf.getTableNameField(), tableName);
        }
    }
    

    static class RowBasedIndexer extends Indexer {
        
        private HTablePool tablePool;
        private Timer rowReadTimer;

        public RowBasedIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper, HTablePool tablePool, SolrInputDocumentWriter solrWriter) {
            super(indexerName, conf, tableName, mapper, solrWriter);
            this.tablePool = tablePool;
            rowReadTimer = Metrics.newTimer(metricName(getClass(), "Row read timer", indexerName), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        }

        private Result readRow(byte[] row) throws IOException {
            TimerContext timerContext = rowReadTimer.time();
            try {
                HTableInterface table = tablePool.getTable(conf.getTable());
                try {
                    Get get = mapper.getGet(row);
                    return table.get(get);
                } finally {
                    table.close();
                }
            } finally {
                timerContext.stop();
            }
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException {

            Map<String, RowData> idToRowData = calculateUniqueEvents(rowDataList);

            for (RowData rowData : idToRowData.values()) {

                Result result = rowData.toResult();
                if (conf.getRowReadMode() == RowReadMode.DYNAMIC) {
                    if (!mapper.containsRequiredData(result)) {
                        result = readRow(rowData.getRow());
                    }
                }

                boolean rowDeleted = result.isEmpty();

                String documentId = uniqueKeyFormatter.formatRow(rowData.getRow());
                if (rowDeleted) {
                    // Delete row from Solr as well
                    updateCollector.deleteById(documentId);
                    if (log.isDebugEnabled()) {
                        log.debug("Row " + Bytes.toString(rowData.getRow()) + ": deleted from Solr");
                    }
                } else {
                    SolrInputDocument document = mapper.map(result);
                    document.addField(conf.getUniqueKeyField(), uniqueKeyFormatter.formatRow(rowData.getRow()));
                    // TODO there should probably be some way for the mapper to indicate there was no useful content to
                    // map,  e.g. if there are no fields in the solrWriter document (and should we then perform a delete instead?)
                    updateCollector.add(documentId, document);
                    addTableName(document);
                    if (log.isDebugEnabled()) {
                        log.debug("Row " + Bytes.toString(rowData.getRow()) + ": added to Solr");
                    }
                }
            }
        }

        /**
         * Calculate a map of Solr document ids to relevant RowData, only taking the most recent event for each document id..
         */
        private Map<String, RowData> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, RowData> idToEvent = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                
                // Check if the event contains changes to relevant key values
                boolean relevant = false;
                for (KeyValue kv : rowData.getKeyValues()) {
                    if (mapper.isRelevantKV(kv) || kv.isDelete()) {
                        relevant = true;
                        break;
                    }
                }

                if (!relevant) {
                    break;
                }
                idToEvent.put(uniqueKeyFormatter.formatRow(rowData.getRow()), rowData);
            }
            return idToEvent;
        }

    }

    static class ColumnBasedIndexer extends Indexer {

        public ColumnBasedIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper, SolrInputDocumentWriter solrWriter) {
            super(indexerName, conf, tableName, mapper, solrWriter);
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException {
            Map<String, KeyValue> idToKeyValue = calculateUniqueEvents(rowDataList);
            for (Entry<String, KeyValue> idToKvEntry : idToKeyValue.entrySet()) {
                String documentId = idToKvEntry.getKey();
                KeyValue keyValue = idToKvEntry.getValue();
                if (idToKvEntry.getValue().isDelete()) {
                    handleDelete(documentId, keyValue, updateCollector);
                } else {
                    Result result = newResult(Collections.singletonList(keyValue));
                    SolrInputDocument document = mapper.map(result);
                    document.addField(conf.getUniqueKeyField(), documentId);
                    
                    addRowAndFamily(document, keyValue);
                    addTableName(document);
                    
                    updateCollector.add(documentId, document);
                }
            }
        }
        
        private void handleDelete(String documentId, KeyValue deleteKeyValue, SolrUpdateCollector updateCollector) {
            byte deleteType = deleteKeyValue.getType();
            if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
                updateCollector.deleteById(documentId);
            } else if (deleteType == KeyValue.Type.DeleteFamily.getCode()) {
                deleteFamily(deleteKeyValue, updateCollector);
            } else if (deleteType == KeyValue.Type.Delete.getCode()) {
                deleteRow(deleteKeyValue, updateCollector);
            } else {
                log.error(String.format("Unknown delete type %d for document %s, not doing anything", deleteType, documentId));
            }
        }

        private void addRowAndFamily(SolrInputDocument document, KeyValue keyValue) {
            if (conf.getRowField() != null) {
                document.addField(conf.getRowField(), uniqueKeyFormatter.formatRow(keyValue.getRow()));
            }

            if (conf.getColumnFamilyField() != null) {
                document.addField(conf.getColumnFamilyField(),
                        uniqueKeyFormatter.formatFamily(keyValue.getFamily()));
            }
        }
        
        
        /**
         * Delete all values for a single column family from Solr.
         */
        private void deleteFamily(KeyValue deleteKeyValue, SolrUpdateCollector updateCollector) {
            String rowField = conf.getRowField();
            String cfField = conf.getColumnFamilyField();
            String rowValue = uniqueKeyFormatter.formatRow(deleteKeyValue.getRow());
            String  familyValue = uniqueKeyFormatter.formatFamily(deleteKeyValue.getFamily());
            if (rowField != null && cfField != null) {
                updateCollector.deleteByQuery(String.format("(%s:%s)AND(%s:%s)", rowField, rowValue, cfField, familyValue));
            } else {
                log.warn(String.format(
                        "Can't delete row %s and family %s from Solr because row and/or family fields not included in the indexer configuration",
                        rowValue, familyValue));
            }
        }

        /**
         * Delete all values for a single row from Solr.
         */
        private void deleteRow(KeyValue deleteKeyValue, SolrUpdateCollector updateCollector) {
            String rowField = conf.getRowField();
            String rowValue = uniqueKeyFormatter.formatRow(deleteKeyValue.getRow());
            if (rowField != null) {
                updateCollector.deleteByQuery(String.format("%s:%s", rowField, rowValue));
            } else {
                log.warn(String.format(
                        "Can't delete row %s from Solr because row field not included in indexer configuration",
                        rowValue));
            }
        }
        
        /**
         * Calculate a map of Solr document ids to KeyValue, only taking the most recent event for each document id.
         */
        private Map<String, KeyValue> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, KeyValue> idToKeyValue = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                for (KeyValue kv : rowData.getKeyValues()) {
                    if (mapper.isRelevantKV(kv)) {
                        String id = uniqueKeyFormatter.formatKeyValue(kv);
                        idToKeyValue.put(id, kv);
                    }
                }
            }
            return idToKeyValue;
        }

    }

}
