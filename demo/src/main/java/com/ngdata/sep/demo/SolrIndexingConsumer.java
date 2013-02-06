package com.ngdata.sep.demo;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.PayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * A consumer that indexes to Solr.
 */
public class SolrIndexingConsumer {
    public static void main(String[] args) throws Exception {
        new SolrIndexingConsumer().run();
    }

    public void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        if (!sepModel.hasSubscription("index1")) {
            sepModel.addSubscription("index1");
        }

        HttpSolrServer solr = new HttpSolrServer("http://localhost:8983/solr");
        TikaSolrCellHBaseMapper mapper = new TikaSolrCellHBaseMapper("http://localhost:8983/solr");

        PayloadExtractor payloadExtractor = new PayloadExtractor(Bytes.toBytes("sep-user-demo"), Bytes.toBytes("info"),
                Bytes.toBytes("payload"));

        SepConsumer sepConsumer = new SepConsumer("index1", System.currentTimeMillis(),
                new Indexer(solr, mapper, conf), 10, "localhost", zk, conf, payloadExtractor);

        sepConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static class Indexer implements EventListener {
        private SolrServer solrServer;
        private TikaSolrCellHBaseMapper mapper;
        private byte[] tableName = Bytes.toBytes("sep-user-demo");
        private HTablePool tablePool;
        private ObjectMapper jsonMapper = new ObjectMapper();

        public Indexer(SolrServer solrServer, TikaSolrCellHBaseMapper mapper, Configuration hbaseConf)
                throws IOException {
            this.solrServer = solrServer;
            this.mapper = mapper;
            tablePool = new HTablePool(hbaseConf, 10);
        }

        @Override
        public void processEvent(SepEvent event) {
            try {
                if (!Bytes.equals(event.getTable(), tableName)) {
                    System.out.println("Got an event for a table we're not interested in: " +
                            Bytes.toString(event.getTable()));
                }

                boolean rowDeleted = false;
                boolean isPartialUpdate = true; // If there is no payload, do the safe thing by re-reading the row
                if (event.getPayload() != null) {
                    MyPayload payload = jsonMapper.readValue(event.getPayload(), MyPayload.class);
                    isPartialUpdate = payload.isPartialUpdate();
                }

                DeleteKind deleteKind = determineDeleteKind(event.getKeyValues());
                switch (deleteKind) {
                    case FAMILY_DELETE:
                        // We might do something smart here, such as verify if data from all the families
                        // used in the mapping has been deleted, and then immediately decide to delete the
                        // corresponding Solr document. For now, be lazy and check back on hbase.
                        isPartialUpdate = true;
                        break;
                    case COLUMN_DELETE:
                        // If there are some deletes amongst the key values, automatically go in partial update
                        // mode, i.e. re-read the row from hbase (if not, we would at least need to filter out
                        // these kv's before passing them to Result)
                        isPartialUpdate = true;
                        break;
                    case NO_DELETE:
                        break;
                    default:
                        throw new RuntimeException("Unexpected delete kind: " + deleteKind);
                }

                Result result = null;
                if (!rowDeleted) {
                    if (isPartialUpdate) {
                        // read row from HBase
                        HTableInterface table = tablePool.getTable(tableName);
                        try {
                            result = table.get(new Get(event.getRow()));
                            if (result.isEmpty()) {
                                rowDeleted = true;
                            }
                        } finally {
                            table.close();
                        }
                    } else {
                        // restore result from key-values supplied through the event
                        result = new Result(event.getKeyValues());
                    }
                }

                if (rowDeleted) {
                    // Delete row from Solr as well
                    // TODO: this has the assumption that the solr unique key is the same as the hbase row key
                    solrServer.deleteById(Bytes.toString(event.getRow()));
                    System.out.println("Row " + Bytes.toString(event.getRow()) + ": deleted from Solr");
                } else {
                    SolrInputDocument solrDoc = mapper.map(result);
                    solrServer.add(solrDoc);
                    System.out.println("Row " + Bytes.toString(event.getRow()) +
                            ": added to Solr, re-read from HBase: " + isPartialUpdate + ", delete kind: " + deleteKind);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        enum DeleteKind {FAMILY_DELETE, COLUMN_DELETE, NO_DELETE }

        private DeleteKind determineDeleteKind(List<KeyValue> keyValues) {
            for (KeyValue kv : keyValues) {
                switch (KeyValue.Type.codeToType(kv.getType())) {
                    case Delete:
                        // A delete of a row is internally translated to deletes of all the families,
                        // so a row-wide delete kv never appears in the hlog
                        throw new RuntimeException("Didn't expect a row-delete kv");
                    case DeleteColumn:
                        return DeleteKind.FAMILY_DELETE;
                    case DeleteFamily:
                        return DeleteKind.COLUMN_DELETE;
                    case Put:
                        break;
                    default:
                        throw new RuntimeException("Unexpected KV type: " + KeyValue.Type.codeToType(kv.getType()));
                }
            }
            return DeleteKind.NO_DELETE;
        }
    }
}
