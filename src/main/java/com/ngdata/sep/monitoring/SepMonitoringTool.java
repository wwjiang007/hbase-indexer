package com.ngdata.sep.monitoring;

import com.google.common.collect.Maps;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.zookeeper.data.Stat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;


// Disclaimer: I don't know enough about HBase replication to know if the information printed by
// this tool is relevant, but it seems to make sense.

public class SepMonitoringTool {
    // peer name, server name
    private Map<String, Map<String, Status>> byPeer = Maps.newHashMap();

    public static void main(String[] args) throws Exception {
        new SepMonitoringTool().run();
    }

    public void run() throws Exception {
        ZooKeeperItf zk = ZkUtil.connect("localhost", 30000);

        String regionServerPath = "/hbase/replication/rs";
        List<String> regionServers = zk.getChildren(regionServerPath, false);

        for (String server : regionServers) {
            String peersPath = regionServerPath + "/" + server;
            List<String> peers = zk.getChildren(peersPath, false);
            for (String peer : peers) {
                String hlogsPath = peersPath + "/" + peer;
                List<String> logs = zk.getChildren(hlogsPath, false);
                for (String log : logs) {
                    Stat stat = new Stat();
                    zk.getData(hlogsPath + "/" + log, false, stat);

                    Map<String, Status> byServer = byPeer.get(peer);
                    if (byServer == null) {
                        byServer = new TreeMap<String, Status>();
                        byPeer.put(peer, byServer);
                    }
                    Status status = byServer.get(server);
                    if (status == null) {
                        status = new Status();
                        byServer.put(server, status);
                    }

                    status.numberOfFiles++;
                    long ctime = stat.getCtime();
                    if (ctime > status.maxCtime) {
                        status.maxCtime = ctime;
                    }
                    if (ctime < status.minCtime) {
                        status.minCtime = ctime;
                    }
                }
            }
        }

        DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

        System.out.println("Columns printed are:");
        System.out.println(" - host");
        System.out.println(" - number of files in queue");
        System.out.println(" - zk node ctime of oldest file");
        System.out.println(" - zk node ctime of youngest file");
        System.out.println();

        for (Map.Entry<String, Map<String, Status>> peerEntry : byPeer.entrySet()) {
            System.out.println();
            System.out.println("Replication peer " + peerEntry.getKey());
            System.out.println();
            for (Map.Entry<String, Status> serverEntry : peerEntry.getValue().entrySet()) {
                Status status = serverEntry.getValue();
                System.out.format("   %1$-50.50s | %2$-10.10s | %3$-30.30s | %4$-30.30s\n", serverEntry.getKey(), String.valueOf(status.numberOfFiles),
                        formatter.print(status.minCtime), formatter.print(status.maxCtime));
            }
        }
        System.out.println();
    }


    public static class Status {
        int numberOfFiles = 0;
        long maxCtime = Long.MIN_VALUE;
        long minCtime = Long.MAX_VALUE;
    }

}
