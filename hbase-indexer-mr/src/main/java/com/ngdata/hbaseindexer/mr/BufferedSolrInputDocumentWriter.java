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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.indexer.SolrInputDocumentWriter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * {@code SolrInputDocumentWriter} decorator that buffers updates and then writes them in batch.
 * <p>
 * Delete operations are not buffered.
 * <p>
 * It is imperative that {@link #flush()} is called at the end of a session with this class in
 * order to flush all remaining buffered writes.
 * <p>
 * <b>WARNING:</b> this class is not thread-safe, and instances should only be accessed by a single thread.
 */
public class BufferedSolrInputDocumentWriter implements SolrInputDocumentWriter {
    
    private final SolrInputDocumentWriter delegateWriter;
    private final int bufferSize;
    private final Map<String, SolrInputDocument> writeBuffer;

    /**
     * Instantiate with the underlying writer to delegate to, and the size of the internal buffer to use.
     * 
     * @param delegateWriter underlying writer to delegate writes and deletes to
     * @param bufferSize size of the internal write buffer to use
     */
    public BufferedSolrInputDocumentWriter(SolrInputDocumentWriter delegateWriter, int bufferSize) {
        this.delegateWriter = delegateWriter;
        this.bufferSize = bufferSize;
        this.writeBuffer = Maps.newHashMapWithExpectedSize(bufferSize);
    }

    @Override
    public void add(Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException {
        writeBuffer.putAll(inputDocumentMap);
        if (writeBuffer.size() >= bufferSize) {
            flush();
        }
    }

    @Override
    public void deleteById(List<String> idsToDelete) throws SolrServerException, IOException {
        delegateWriter.deleteById(idsToDelete);
    }

    @Override
    public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
        delegateWriter.deleteByQuery(deleteQuery);
    }
    
    /**
     * Flush all buffered documents to the underlying writer.
     */
    public void flush() throws SolrServerException, IOException {
        if (!writeBuffer.isEmpty()) {
            delegateWriter.add(ImmutableMap.copyOf(writeBuffer));
            writeBuffer.clear();
        }
    }
    
    @Override
    public void close() throws SolrServerException, IOException {
        flush();
        delegateWriter.close();
    }

}
