package com.ngdata.sep.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.ngdata.util.concurrent.WaitPolicy;

import com.ngdata.util.io.Closer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepHBaseSchema.RecordCf;
import com.ngdata.sep.impl.SepHBaseSchema.RecordColumn;
import com.ngdata.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class SepEventSlave extends BaseHRegionServer {
    private final String subscriptionId;
    private long subscriptionTimestamp;
    private final EventListener listener;
    private final String hostName;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private RpcServer rpcServer;
    private final int threadCnt;
    private List<ThreadPoolExecutor> executors;
    private HashFunction hashFunction = Hashing.murmur3_32();
    private SepMetrics sepMetrics;
    private String zkNodePath;
    boolean running = false;
    private Log log = LogFactory.getLog(getClass());

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not inactive)
     * @param listener listeners that will process the events
     * @param threadCnt number of worker threads that will handle incoming SEP events
     * @param hostName hostname to bind to
     */
    public SepEventSlave(String subscriptionId, long subscriptionTimestamp, EventListener listener, int threadCnt,
            String hostName, ZooKeeperItf zk, Configuration hbaseConf) {
        Preconditions.checkArgument(threadCnt > 0, "Thread count must be > 0");
        this.subscriptionId = SepModelImpl.toInternalSubscriptionName(subscriptionId);
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.listener = listener;
        this.hostName = hostName;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.threadCnt = threadCnt;

        this.executors = new ArrayList<ThreadPoolExecutor>(threadCnt);
        for (int i = 0; i < threadCnt; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
        this.sepMetrics = new SepMetrics(subscriptionId);
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        // TODO see same call in HBase's HRegionServer:
        //   - should we do HBaseRPCErrorHandler ?
        rpcServer = HBaseRPC.getServer(this,
                new Class<?>[] { HRegionInterface.class },
                hostName,
                0, /* ephemeral port */
                10, // TODO how many handlers do we need? make it configurable?
                10,
                false, // TODO make verbose flag configurable
                hbaseConf,
                0); // TODO need to check what this parameter is for

        rpcServer.start();

        int port = rpcServer.getListenerAddress().getPort();

        // Publish our existence in ZooKeeper
        // See HBase ServerName class: format of server name is: host,port,startcode
        // Startcode is to distinguish restarted servers on same hostname/port
        String serverName = hostName + "," + port + "," + System.currentTimeMillis();
        zkNodePath = SepModel.HBASE_ROOT + "/" + subscriptionId + "/rs/" + serverName;
        zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        
        this.running = true;
    }

    public void stop() {
        if (running) {
            running = false;
            Closer.close(rpcServer);
            try {
                // This ZK node will likely already be gone if the index has been removed
                // from ZK, but we'll try to remove it here to be sure
                zk.delete(zkNodePath, -1);
            } catch (Exception e) {
                log.debug("Exception while removing zookeeper node", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
	    sepMetrics.shutdown();
    }
    
    public boolean isRunning() {
        return running;
    }

    @Override
    public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
        // TODO quickly hacked in the multi-threading: should maybe approach this differently
        List<Future<?>> futures = new ArrayList<Future<?>>();

        // TODO Recording of last processed timestamp won't work if two batches of log entries are sent out of order
        long lastProcessedTimestamp = -1;
        
        nextEntry: for (HLog.Entry entry : entries) {
            HLogKey entryKey = entry.getKey();
            if (!Bytes.equals(entryKey.getTablename(), SepHBaseSchema.RECORD_TABLE) || entryKey.getWriteTime() < subscriptionTimestamp) {
                continue;
            }
            for (final KeyValue kv : entry.getEdit().getKeyValues()) {
                if (kv.matchingColumn(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes)) {
                    // We don't want messages of the same row to be processed concurrently, therefore choose
                    // a thread based on the hash of the row key
                    int partition = (hashFunction.hashBytes(kv.getRow()).asInt() & Integer.MAX_VALUE) % threadCnt;
                    Future<?> future = executors.get(partition).submit(new Runnable() {
                        @Override
                        public void run() {
                            long before = System.currentTimeMillis();
                            log.debug("Delivering message to listener");
                            listener.processMessage(kv.getRow(), kv.getValue());
                            sepMetrics.reportFilteredSepOperation(System.currentTimeMillis() - before);
                        }
                    });
                    futures.add(future);
                    lastProcessedTimestamp = Math.max(lastProcessedTimestamp, entry.getKey().getWriteTime());
                    continue nextEntry;
                }
            }

            if (log.isInfoEnabled()) {
                // TODO this might not be unusual
                log.info("No payload found in " + entry.toString());
            }
        }

        // We should wait for all operations to finish before returning, because otherwise HBase might
        // deliver a next batch from the same HLog to a different server. This becomes even more important
        // if an exception has been thrown in the batch, as waiting for all futures increases the back-off that
        // occurs before the next attempt
        List<Exception> exceptionsThrown = Lists.newArrayList();
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in processing events.", e);
            } catch (Exception e) {
                exceptionsThrown.add(e);
            }
        }
        
        if (!exceptionsThrown.isEmpty()) {
            log.error("Encountered exceptions on " + exceptionsThrown.size() + " edits (out of " + futures.size() + " total edits)");
            throw new RuntimeException(exceptionsThrown.get(0));
        }
        
        if (lastProcessedTimestamp > 0) {
            sepMetrics.reportSepTimestamp(lastProcessedTimestamp);
        }
    }
}
