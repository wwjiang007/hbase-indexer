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
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * Factory for two SolrInputDocumentWriter classes Solr cloud and Solr classic
 * <p>
 * Implementations of this class may write directly to Solr, or to any other underlying
 * store than can handle SolrInputDocuments.
 */
public class SolrInputDocumentWriterFactory {

    /**
     * Creates a SolrInputDocumentWriter targeting Solr cloud.
     */
    public DirectSolrInputDocumentWriter createSolrCloudWriter(String indexName, SolrServer solrServer) throws MalformedURLException {
        return new DirectSolrInputDocumentWriter(indexName, solrServer);
    }

    /**
     * Creates a SolrInputDocumentWriter targeting Solr classic.
     */
    public DirectSolrClassicInputDocumentWriter createSolrClassicWriter(String indexName, List<SolrServer> solrServers) throws MalformedURLException {
        return new DirectSolrClassicInputDocumentWriter(indexName, solrServers);
    }

}