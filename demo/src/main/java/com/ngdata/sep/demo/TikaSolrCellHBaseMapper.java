package com.ngdata.sep.demo;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.ByteBufferOutputStream;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.handler.extraction.ExtractingMetadataConstants;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps an HBase Result object to a Solr document using Tika and Solr Cell.
 */
public class TikaSolrCellHBaseMapper {
    private AutoDetectParser parser;
    private IndexSchema schema;

    public TikaSolrCellHBaseMapper(String solrUrl) throws Exception {
        // Solr Cell needs a Solr IndexSchema object, for this we just figure out the location on the filesystem
        // of the default core and assume we can access these files locally.
        // TODO we can't make these assumptions
        String coresUrl = solrUrl + "/admin/cores?wt=json";
        ObjectNode node = new ObjectMapper().readValue(new URL(coresUrl), ObjectNode.class);
        String instanceDir = node.get("status").get(node.get("defaultCoreName").getTextValue()).get("instanceDir").getTextValue();

        InputStream is = new FileInputStream(instanceDir + "/conf/solrconfig.xml");
        SolrConfig config = new SolrConfig(instanceDir, "demo", new InputSource(is));
        is.close();

        is = new FileInputStream(instanceDir + "/conf/schema.xml");
        schema = new IndexSchema(config, "demo", new InputSource(is));
        is.close();

        // Make a Tika AutoDetectParser to which we add our HBaseParser
        TikaConfig tikaConfig = new TikaConfig();
        parser = new AutoDetectParser(tikaConfig);

        // TODO: should figure out the right way to register a custom parser
        HBaseParser hbaseParser = new HBaseParser();
        Map<MediaType, Parser> parsers = parser.getParsers();
        for (MediaType type : hbaseParser.getSupportedTypes(new ParseContext())) {
            parsers.put(type, hbaseParser);
        }
        parser.setParsers(parsers);
    }

    public SolrInputDocument map(Result result) throws TikaException, SAXException, IOException {
        // Tika metadata, this is both input and output
        Metadata metadata = new Metadata();
        metadata.add(HttpHeaders.CONTENT_TYPE, "application/hbase-result");
        metadata.add(ExtractingMetadataConstants.STREAM_CONTENT_TYPE, "application/hbase-result");

        // Setup Solr Cell, which is a SAX content handler to which Tika sends its output
        Map<String, String> cellParams = new HashMap<String, String>();
        cellParams.put("uprefix", "ignored_");
        cellParams.put("fmap.row", "id");
        SolrContentHandler handler = new SolrContentHandler(metadata, new MapSolrParams(cellParams), schema);

        // Convert the HBase Result to an InputStream to pass it to the Tika parser
        // TODO: alternative idea: put the Result object in the ParserContext? But ParserContext is intended
        //       for information not related to a specific document.
        InputStream is;
        ImmutableBytesWritable bytes = result.getBytes();
        if (bytes != null) {
            is = new ByteArrayInputStream(bytes.get(), bytes.getOffset(), bytes.getLength());
        } else {
            ByteBufferOutputStream os = new ByteBufferOutputStream((int)result.getWritableSize());
            DataOutputStream out = new DataOutputStream(os);
            result.write(out);
            out.flush();
            ByteBuffer buffer = os.getByteBuffer();
            // Result.write prepends the output with the total length, while the parse code (Result.readFields)
            // doesn't expect the length to be there, therefore, we strip of the starting integer.
            is = new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + Bytes.SIZEOF_INT,
                    buffer.remaining() - Bytes.SIZEOF_INT);
        }

        // Call Tika
        parser.parse(is, handler, metadata, new ParseContext());

        return handler.newDocument();
    }
}
