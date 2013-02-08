/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.demo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * A Tika parser for parsing serialized HBase Result objects (= serialized List&lt;KeyValue>).
 */
public class HBaseParser extends AbstractParser {
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return Collections.singleton(MediaType.application("hbase-result"));
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        try {
            // Reconstruct result object
            byte[] data = IOUtils.toByteArray(stream);
            Result result = new Result(new ImmutableBytesWritable(data));

            // Add row key
            metadata.add("row", Bytes.toString(result.getRow()));

            // Add all columns in all families (latest version only)
            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> rowEntry : result.getNoVersionMap().entrySet()) {
                String family = Bytes.toString(rowEntry.getKey());
                for (Map.Entry<byte[], byte[]> familyEntry : rowEntry.getValue().entrySet()) {
                    byte[] column = familyEntry.getKey();
                    byte[] value = familyEntry.getValue();
                    // TODO: I appended _en here just to have an existing solr dynamnic field
                    metadata.add(family + "_" + Bytes.toString(column) + "_en", Bytes.toString(value));
                }
            }

            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.endDocument();
        } catch (Exception e) {
            // TODO: it seems like the full stack trace is nowhere printed (this is an exception thrown
            // to the sep and then to hbase), therefore I added this for now
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
