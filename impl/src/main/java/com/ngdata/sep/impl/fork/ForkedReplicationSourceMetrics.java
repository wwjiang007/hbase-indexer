/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl.fork;

import org.apache.hadoop.hbase.metrics.MetricsRate;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceMetrics;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatistics;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * Forked version of HBase's ReplicationSourceMetrics to add some extra metrics. Changed are marked with "SEP change".
 */
public class ForkedReplicationSourceMetrics implements Updater {
    private final MetricsRecord metricsRecord;
    private MetricsRegistry registry = new MetricsRegistry();

    /** Rate of shipped operations by the source */
    public final MetricsRate shippedOpsRate =
            new MetricsRate("shippedOpsRate", registry);

    /** Rate of shipped batches by the source */
    public final MetricsRate shippedBatchesRate =
            new MetricsRate("shippedBatchesRate", registry);

    /** Rate of log entries (can be multiple Puts) read from the logs */
    public final MetricsRate logEditsReadRate =
            new MetricsRate("logEditsReadRate", registry);

    /** Rate of log entries filtered by the source */
    public final MetricsRate logEditsFilteredRate =
            new MetricsRate("logEditsFilteredRate", registry);

    /** Age of the last operation that was shipped by the source */
    private final MetricsLongValue ageOfLastShippedOp =
            new MetricsLongValue("ageOfLastShippedOp", registry);

    /**
     * Current size of the queue of logs to replicate,
     * excluding the one being processed at the moment
     */
    public final MetricsIntValue sizeOfLogQueue =
            new MetricsIntValue("sizeOfLogQueue", registry);

    // It's a little dirty to preset the age to now since if we fail
    // to replicate the very first time then it will show that age instead
    // of nothing (although that might not be good either).
    private long lastTimestampForAge = System.currentTimeMillis();

    // SEP change
    private final MetricsIntValue selectedPeerCount =
            new MetricsIntValue("ngdata_selectedPeerCount", registry);

    // SEP change
    // This is not useful as a metric per se, but is interesting to be able to read out via JMX
    private final MetricsLongValue timestampLastShippedOp =
            new MetricsLongValue("ngdata_timestampLastShippedOp", registry);

    /**
     * Constructor used to register the metrics
     * @param id Name of the source this class is monitoring
     */
    public ForkedReplicationSourceMetrics(String id) {
        MetricsContext context = MetricsUtil.getContext("hbase");
        String name = Thread.currentThread().getName();
        metricsRecord = MetricsUtil.createRecord(context, "replication");
        metricsRecord.setTag("RegionServer", name);
        context.registerUpdater(this);
        try {
            id = URLEncoder.encode(id, "UTF8");
        } catch (UnsupportedEncodingException e) {
            id = "CAN'T ENCODE UTF8";
        }
        // export for JMX
        new ReplicationStatistics(this.registry, "ReplicationSource for " + id);
    }

    /**
     * Set the age of the last edit that was shipped
     * @param timestamp write time of the edit
     */
    public void setAgeOfLastShippedOp(long timestamp) {
        lastTimestampForAge = timestamp;
        ageOfLastShippedOp.set(System.currentTimeMillis() - lastTimestampForAge);
        // SEP change
        timestampLastShippedOp.set(timestamp);
    }

    /**
     * Convenience method to use the last given timestamp to refresh the age
     * of the last edit. Used when replication fails and need to keep that
     * metric accurate.
     */
    public void refreshAgeOfLastShippedOp() {
        setAgeOfLastShippedOp(lastTimestampForAge);
    }

    // SEP change
    public void setSelectedPeerCount(int count) {
        selectedPeerCount.set(count);
    }

    @Override
    public void doUpdates(MetricsContext metricsContext) {
        synchronized (this) {
            this.shippedOpsRate.pushMetric(this.metricsRecord);
            this.shippedBatchesRate.pushMetric(this.metricsRecord);
            this.logEditsReadRate.pushMetric(this.metricsRecord);
            this.logEditsFilteredRate.pushMetric(this.metricsRecord);
            this.ageOfLastShippedOp.pushMetric(this.metricsRecord);
            this.sizeOfLogQueue.pushMetric(this.metricsRecord);
            // SEP change
            this.selectedPeerCount.pushMetric(this.metricsRecord);
            this.timestampLastShippedOp.pushMetric(this.metricsRecord);
        }
        this.metricsRecord.update();
    }
}
