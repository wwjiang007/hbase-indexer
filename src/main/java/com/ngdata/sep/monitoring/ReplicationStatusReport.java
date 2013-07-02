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

import java.io.PrintStream;
import java.text.DecimalFormat;

/**
 * Generate a text report of the replication status.
 */
public class ReplicationStatusReport {
    public static void printReport(ReplicationStatus replicationStatus, PrintStream out) {
        if (replicationStatus.getPeersAndRecoveredQueues().size() == 0) {
            System.out.println("There are no peer clusters.");
            return;
        }

        String columnFormat = "  | %1$-50.50s | %2$-15.15s | %3$-15.15s | %4$-15.15s | %5$-15.15s |\n";

        out.println();
        out.println("Some notes on the displayed information:");
        out.println(" * we don't know the size, and hence the progress, of the HLog which is");
        out.println("   currently being written. But, if the queue always stays at size 1, you");
        out.println("   are in pretty good shape.");
        out.println(" * age of last shipped op: this is the age of the last shipped wal entry,");
        out.println("   at the time it was shipped. If there is no further activity on HBase,");
        out.println("   this value will stay constant.");
        out.println(" * not all entries in the HLogs are of interest to every peer: therefore,");
        out.println("   a large and slowly progressing queue might suddenly quickly shrink to 1.");
        out.println();

        out.format(columnFormat, "Host", "Queue size",      "Size all HLogs",  "Current HLog", "Age last");
        out.format(columnFormat, "",     "(incl. current)", "(excl. current)", "progress",     "shipped op");

        for (String peerId : replicationStatus.getPeersAndRecoveredQueues()) {
            out.println();
            if (replicationStatus.isRecoveredQueue(peerId)) {
                out.println("Recovered queue: " + peerId);
            } else {
                out.println("Peer cluster: " + peerId);
            }
            out.println();
            for (String server : replicationStatus.getServers(peerId)) {
                ReplicationStatus.Status status = replicationStatus.getStatus(peerId, server);
                out.format(columnFormat, server,
                        String.valueOf(status.getHLogCount()), formatAsMB(status.getTotalHLogSize()),
                        formatProgress(status.getProgressOnCurrentHLog()), formatDuration(status.ageOfLastShippedOp));
            }
        }
        out.println();
    }

    private static String formatAsMB(long size) {
        if (size == -1) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("#.# MB");
            return format.format((double)size / 1000d / 1000d);
        }
    }

    private static String formatProgress(float progress) {
        if (Float.isNaN(progress)) {
            return "unknown";
        } else {
            DecimalFormat format = new DecimalFormat("0 %");
            return format.format(progress);
        }
    }

    private static String formatDuration(Long millis) {
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
}
