package com.ngdata.sep.demo;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepEventSlave;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.zookeeper.ZkUtil;
import com.ngdata.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoIndexer {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        if (!sepModel.hasSubscription("index1")) {
            sepModel.addSubscription("index1");
        }

        SepEventSlave eventSlave = new SepEventSlave("index1", System.currentTimeMillis(),
                new Indexer(), 10, "localhost", zk, conf);

        eventSlave.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static class Indexer implements EventListener {
        @Override
        public void processMessage(byte[] row, byte[] payload) {
            System.out.println("Received event for row " + Bytes.toString(row));
        }
    }
}
