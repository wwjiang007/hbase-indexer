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
package com.ngdata.sep.monitoring;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


public class SepMonitoringTool {
    private ZooKeeperItf zk;
    private FileSystem fileSystem;
    private Path hbaseRootDir;
    private Path hbaseOldLogDir;
    private static final int HBASE_JMX_PORT = 10102;

    public static void main(String[] args) throws Exception {
        new SepMonitoringTool().run(args);
    }

    public void run(String[] args) throws Exception {
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(SepMonitoringTool.class.getResource("log4j.properties"));

        OptionParser parser =  new OptionParser();
        OptionSpec enableJmxOption = parser.accepts("enable-jmx",
                "use JMX to retrieve info from HBase regionservers (port " + HBASE_JMX_PORT + ")");
        OptionSpec<String> zkOption = parser
                .acceptsAll(Lists.newArrayList("z"), "ZooKeeper connection string, defaults to localhost")
                .withRequiredArg().ofType(String.class)
                .defaultsTo("localhost");

        OptionSet options = null;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            System.out.println("Error parsing command line options:");
            System.out.println(e.getMessage());
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        boolean enableJmx = options.has(enableJmxOption);
        String zkConnectString = options.valueOf(zkOption);

        System.out.println("Connecting to Zookeeper " + zkConnectString + "...");
        zk = ZkUtil.connect(zkConnectString, 30000);

        Configuration conf = getHBaseConf(zk);

        if (!"true".equalsIgnoreCase(conf.get("hbase.replication"))) {
            System.out.println("HBase replication is not enabled.");
            return;
        }

        fileSystem = FileSystem.get(conf);
        hbaseRootDir = FSUtils.getRootDir(conf);
        hbaseOldLogDir = new Path(hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);

        Map<String, Map<String, Status>> statusByPeerAndServer = collectStatusFromZooKeepeer();

        if (statusByPeerAndServer.size() == 0) {
            System.out.println("There are no peer clusters.");
            return;
        }

        if (enableJmx) {
            addStatusFromJmx(statusByPeerAndServer);
        } else {
            System.out.println();
            System.out.println("Hint: use --enable-jmx to retrieve more info from HBase regionservers");
            System.out.println("      For this, you need to start HBase regionservers with:");
            System.out.println("      -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=10102");
            System.out.println();
        }

        printReport(statusByPeerAndServer);
    }

    private void printReport(Map<String, Map<String, Status>> statusByPeerAndServer) {
        String columnFormat = "  | %1$-50.50s | %2$-15.15s | %3$-15.15s | %4$-15.15s | %5$-15.15s |\n";

        System.out.println();
        System.out.println("Some notes on the displayed information:");
        System.out.println(" * we don't know the size, and hence the progress, of the HLog which is");
        System.out.println("   currently being written. But, if the queue always stays at size 1, you");
        System.out.println("   are in pretty good shape.");
        System.out.println(" * age of last shipped op: this is the age of the last shipped wal entry,");
        System.out.println("   at the time it was shipped. If there is no further activity on HBase,");
        System.out.println("   this value will stay constant.");
        System.out.println(" * not all entries in the HLogs are of interest to every peer: therefore,");
        System.out.println("   a large and slowly progressing queue might suddenly quickly shrink to 1.");
        System.out.println();

        System.out.format(columnFormat, "Host", "Queue size",      "Size all HLogs",  "Current HLog", "Age last");
        System.out.format(columnFormat, "",     "(incl. current)", "(excl. current)", "progress",     "shipped op");

        for (Map.Entry<String, Map<String, Status>> peerEntry : statusByPeerAndServer.entrySet()) {
            System.out.println();
            String peerId = peerEntry.getKey();
            if (peerId.contains("-")) {
                // For recovered queues, there is a zk node with the format "peerId(-servername)+"
                System.out.println("Recovered queue: " + peerEntry.getKey());
            } else {
                System.out.println("Peer cluster: " + peerEntry.getKey());
            }
            System.out.println();
            for (Map.Entry<String, Status> serverEntry : peerEntry.getValue().entrySet()) {
                Status status = serverEntry.getValue();
                System.out.format(columnFormat, serverEntry.getKey(),
                        String.valueOf(status.getHLogCount()), formatAsMB(status.getTotalHLogSize()),
                        formatProgress(status.getProgressOnCurrentHLog()), formatDuration(status.ageOfLastShippedOp));
            }
        }
        System.out.println();
    }

    private Map<String, Map<String, Status>> collectStatusFromZooKeepeer() throws Exception {
        Map<String, Map<String, Status>> statusByPeerAndServer = Maps.newHashMap();

        String regionServerPath = "/hbase/replication/rs";
        List<String> regionServers = zk.getChildren(regionServerPath, false);

        for (String server : regionServers) {
            String peersPath = regionServerPath + "/" + server;
            List<String> peers = zk.getChildren(peersPath, false);
            for (String peer : peers) {
                // The peer nodes are either real peers or recovered queues, we make no distinction for now
                String hlogsPath = peersPath + "/" + peer;
                // The hlogs are not correctly sorted when we get them from ZK
                SortedSet<String> logs = new TreeSet<String>(Collections.reverseOrder());
                logs.addAll(zk.getChildren(hlogsPath, false));
                for (String log : logs) {
                    Map<String, Status> statusByServer = statusByPeerAndServer.get(peer);
                    if (statusByServer == null) {
                        statusByServer = new TreeMap<String, Status>();
                        statusByPeerAndServer.put(peer, statusByServer);
                    }
                    Status status = statusByServer.get(server);
                    if (status == null) {
                        status = new Status();
                        statusByServer.put(server, status);
                    }

                    Stat stat = new Stat();
                    byte[] data = zk.getData(hlogsPath + "/" + log, false, stat);

                    // Determine position in hlog, if already started on the hlog
                    long position = -1;
                    if (data != null && data.length > 0) {
                        data = removeMetaData(data);
                        position = Long.parseLong(new String(data, "UTF-8"));
                    }

                    HLogInfo hlogInfo = new HLogInfo(log);
                    hlogInfo.size = getLogFileSize(server, log);
                    hlogInfo.position = position;
                    status.hlogs.add(hlogInfo);
                }
            }
        }

        return statusByPeerAndServer;
    }

    private void addStatusFromJmx(Map<String, Map<String, Status>> statusByPeerAndServer) throws Exception {
        JmxConnections jmxConnections = new JmxConnections();

        for (Map.Entry<String, Map<String, Status>> peerEntry : statusByPeerAndServer.entrySet()) {
            String peerId = peerEntry.getKey();
            for (Map.Entry<String, Status> serverEntry : peerEntry.getValue().entrySet()) {
                String server = serverEntry.getKey();
                Status status = serverEntry.getValue();
                String hostName = ServerName.parseHostname(server);

                MBeanServerConnection connection = jmxConnections.getConnector(hostName, HBASE_JMX_PORT).getMBeanServerConnection();

                ObjectName replSourceBean = new ObjectName("hadoop:service=Replication,name=ReplicationSource for " + URLEncoder.encode(peerId, "UTF8"));
                try {
                    status.ageOfLastShippedOp = (Long)connection.getAttribute(replSourceBean, "ageOfLastShippedOp");
                } catch (AttributeNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                } catch (InstanceNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                }
            }
        }

        jmxConnections.close();
    }

    private Configuration getHBaseConf(ZooKeeperItf zk) throws KeeperException, InterruptedException, IOException {
        // Read the HBase/Hadoop configuration via the master web ui
        // This is debatable, but it avoids any pitfalls with conf dirs and also works with launch-test-lily
        byte[] masterServerName = removeMetaData(zk.getData("/hbase/master", false, new Stat()));
        String hbaseMasterHostName = ServerName.parseVersionedServerName(masterServerName).getHostname();

        String url = "http://" + hbaseMasterHostName + ":60010/conf";
        System.out.println("Reading HBase configuration from " + url);
        byte[] data = readUrl(url);

        Configuration conf = new Configuration();
        conf.addResource(new ByteArrayInputStream(data));

        return conf;
    }

    private byte[] readUrl(String url) throws IOException {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(url);

        HttpResponse response = httpclient.execute(httpGet);

        try {
            HttpEntity entity = response.getEntity();
            return IOUtils.toByteArray(entity.getContent());
        } finally {
            if (response.getEntity() != null) {
                EntityUtils.consume(response.getEntity());
            }
            httpGet.releaseConnection();
        }
    }

    private String formatAsMB(long size) {
        if (size == -1) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("#.# MB");
            return format.format((double)size / 1000d / 1000d);
        }
    }

    private String formatProgress(float progress) {
        if (Float.isNaN(progress)) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("0 %");
            return format.format(progress);
        }
    }

    private String formatDuration(Long millis) {
        if (millis == null) {
            return "(enable jmx)";
        }

        long millisOverflow = millis % 1000;
        long seconds = (millis - millisOverflow) / 1000;
        long secondsOverflow = seconds % 60;
        long minutes = (seconds - secondsOverflow) / 60;
        long minutesOverflow = minutes % 60;
        long hours = (minutes - minutesOverflow) / 60;
        int days = (int)Math.floor((double)hours / 24d);

        return String.format("%1$sd %2$02d:%3$02d:%4$02d.%5$03d",
                days, hours, minutesOverflow, secondsOverflow, millisOverflow);
    }

    /**
     *
     * @param serverName the 'unique-over-restarts' name, i.e. hostname with start code suffix
     * @param hlogName name of HLog
     */
    private long getLogFileSize(String serverName, String hlogName) throws IOException {
        Path hbaseLogDir = new Path(hbaseRootDir, HLog.getHLogDirectoryName(serverName));
        Path path = new Path(hbaseLogDir, hlogName);
        try {
            FileStatus status = fileSystem.getFileStatus(path);
            return status.getLen();
        } catch (FileNotFoundException e) {
            Path oldLogPath = new Path(hbaseOldLogDir, hlogName);
            try {
                return fileSystem.getFileStatus(oldLogPath).getLen();
            } catch (FileNotFoundException e2) {
                System.err.println("HLog not found at : " + path + " or " + oldLogPath);
                return -1;
            }
        }
    }

    public static class Status {
        List<HLogInfo> hlogs = new ArrayList<HLogInfo>();
        Long ageOfLastShippedOp;

        int getHLogCount() {
            int count = 0;
            for (HLogInfo hlog : hlogs) {
                count++;
                if (hlog.position != -1) {
                    // we arrived at the current hlog file
                    // Apparently, HBase (0.94) keeps one more older hlog file around, that is already fully processed,
                    // and we can ignore that one.
                    break;
                }
            }
            return count;
        }

        long getTotalHLogSize() {
            long totalSize = 0;
            for (HLogInfo hlog : hlogs) {
                totalSize += hlog.size;
                if (hlog.position != -1) {
                    // we arrived at the current hlog
                    break;
                }
            }
            return totalSize;
        }

        float getProgressOnCurrentHLog() {
            for (HLogInfo hlog : hlogs) {
                if (hlog.position != -1) {
                    if (hlog.size > 0) {
                        return (float)hlog.position / (float)hlog.size;
                    } else {
                        return Float.NaN;
                    }
                }
            }
            return Float.NaN;
        }
    }

    public static class HLogInfo {
        String name;
        /** Currently reached position in the file, -1 if unstarted. */
        long position;
        /** Size of the HLog. Note that for files being written, this only includes the size of completed blocks. */
        long size;

        public HLogInfo(String name) {
            this.name = name;
        }
    }

    private static final byte MAGIC =(byte) 0XFF;
    private static final int MAGIC_SIZE = Bytes.SIZEOF_BYTE;
    private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
    private static final int ID_LENGTH_SIZE =  Bytes.SIZEOF_INT;

    /** This method was copied from RecoverableZooKeeper in the HBase 0.94 source tree. */
    public byte[] removeMetaData(byte[] data) {
        if(data == null || data.length == 0) {
            return data;
        }
        // check the magic data; to be backward compatible
        byte magic = data[0];
        if(magic != MAGIC) {
            return data;
        }

        int idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
        int dataLength = data.length-MAGIC_SIZE-ID_LENGTH_SIZE-idLength;
        int dataOffset = MAGIC_SIZE+ID_LENGTH_SIZE+idLength;

        byte[] newData = new byte[dataLength];
        System.arraycopy(data, dataOffset, newData, 0, dataLength);
        return newData;
    }
}
