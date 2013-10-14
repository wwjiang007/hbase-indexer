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

import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import org.apache.solr.common.SolrInputDocument;

/**
 * SolrUpdateWriter that adds a single document id to Solr documents if they are not available.
 */
public class IdAddingSolrUpdateWriter implements SolrUpdateWriter {
    
    private final String uniqueKeyField;
    private final String documentId;
    private final SolrUpdateCollector updateCollector;
    private boolean idUsed = false;
    
    /**
     * Construct with the document id field and doc id to be added when necessary.
     * 
     * @param uniqueKeyField name of the Solr unique key field
     * @param documentId identifier to be used for documents written to this writer.
     * @param updateCollector collector to which documents are passed through to
     */
    public IdAddingSolrUpdateWriter(String uniqueKeyField, String documentId, SolrUpdateCollector updateCollector) {
        this.uniqueKeyField = uniqueKeyField;
        this.documentId = documentId;
        this.updateCollector = updateCollector;
    }

    /**
     * Add a SolrInputDocument to this writer.
     * <p>
     * Adding multiple documents without ids will result in an IllegalStateException being thrown.
     */
    @Override
    public void add(SolrInputDocument solrDocument) {
        if (solrDocument.getField(uniqueKeyField) == null) {
            if (idUsed) {
                throw new IllegalStateException("Document id '" + documentId + "' has already been used by this record");
            }
            solrDocument.addField(uniqueKeyField, documentId);
            idUsed = true;
        }
        updateCollector.add(solrDocument);
    }

}
