package com.ngdata.sep.demo;

import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A simple consumer that just logs the events.
 */
public class LoggingConsumer {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        final String subscriptionName = "logger";

        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }

        SepConsumer sepConsumer = new SepConsumer(subscriptionName, System.currentTimeMillis(),
                new Indexer(), 10, "localhost", zk, conf);

        sepConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static class Indexer implements EventListener {
        @Override
        public void processEvent(SepEvent sepEvent) {
            System.out.println("Received event for row " + Bytes.toString(sepEvent.getRow()));
        }
    }
}
