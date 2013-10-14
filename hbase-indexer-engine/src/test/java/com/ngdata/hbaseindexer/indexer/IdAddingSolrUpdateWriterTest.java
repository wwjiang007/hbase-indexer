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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Before;
import org.junit.Test;

public class IdAddingSolrUpdateWriterTest {

    private final String UNIQUE_KEY_FIELD = "_unique_key_field_";
    private final String DOCUMENT_ID = "_doc_id_";

    private SolrInputDocument solrDoc;
    private SolrUpdateCollector updateCollector;
    private IdAddingSolrUpdateWriter updateWriter;

    @Before
    public void setUp() {
        solrDoc = mock(SolrInputDocument.class);
        updateCollector = mock(SolrUpdateCollector.class);
        updateWriter = new IdAddingSolrUpdateWriter(UNIQUE_KEY_FIELD, DOCUMENT_ID, updateCollector);
    }

    @Test
    public void testAdd_AddId() {
        updateWriter.add(solrDoc);

        verify(solrDoc).addField(UNIQUE_KEY_FIELD, DOCUMENT_ID);
        verify(updateCollector).add(solrDoc);
    }

    @Test
    public void testAdd_IdAlreadyPresent() {
        when(solrDoc.getField(UNIQUE_KEY_FIELD)).thenReturn(new SolrInputField(DOCUMENT_ID));

        updateWriter.add(solrDoc);

        verify(updateCollector).add(solrDoc);
    }

    // Adding two documents without ids to the same update writer isn't allowed because
    // it would only result in a single document in Solr
    @Test(expected = IllegalStateException.class)
    public void testAdd_MultipleDocumentsForOneId() {
        updateWriter.add(solrDoc);
        updateWriter.add(solrDoc);
    }
    
    @Test
    public void testAdd_MultipleDocumentsWithTheirOwnIds() {
        String idA = DOCUMENT_ID + "A";
        String idB = DOCUMENT_ID + "B";
        
        SolrInputDocument docA = mock(SolrInputDocument.class);
        SolrInputDocument docB = mock(SolrInputDocument.class);
        
        when(docA.getField(UNIQUE_KEY_FIELD)).thenReturn(new SolrInputField(idA));
        when(docB.getField(UNIQUE_KEY_FIELD)).thenReturn(new SolrInputField(idB));
        

        updateWriter.add(docA);
        updateWriter.add(docB);

        verify(updateCollector).add(docA);
        verify(updateCollector).add(docB);
    }

}
