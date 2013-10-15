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
package com.ngdata.hbaseindexer.mr;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.solr.hadoop.ForkedMapReduceIndexerTool.OptionsBridge;
import org.apache.solr.hadoop.MapReduceIndexerTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for commandline options passed in for HBase Indexer, as well as a bridge to existing MapReduce index
 * building functionality.
 */
class HBaseIndexingOptions extends OptionsBridge {

    private static final Logger LOG = LoggerFactory.getLogger(MapReduceIndexerTool.class);

    private Configuration conf;
    private Scan scan;
    // Flag that we have created our own output directory
    private boolean generatedOutputDir = false;

    public String indexerZkHost;
    public String indexName = "unnamed-index";
    public File hbaseIndexerConfig;
    public String hbaseTableName;
    public String startRow;
    public String endRow;
    public Long startTime;
    public Long endTime;

    public HBaseIndexingOptions(Configuration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    /**
     * Determine if this is intended to be a map-only job that writes directly to a live Solr server.
     * 
     * @return true if writes are to be done directly into Solr
     */
    public boolean isDirectWrite() {
        return reducers == 0;
    }

    public Scan getScan() {
        if (scan == null) {
            throw new IllegalStateException("Scan has not yet been evaluated");
        }
        return scan;
    }

    /**
     * Evaluate the current state to calculate derived options settings. Validation of the state is also done here, so
     * IllegalStateException will be thrown if incompatible options have been set.
     * 
     * @throws IllegalStateException if an illegal combination of options has been set, or needed options are missing
     */
    public void evaluate() {
        evaluateOutputDir();
        evaluateScan();
        evaluateGoLiveArgs();
        evaluateNumReducers();
    }

    /**
     * Check the existence of an output directory setting if needed, or setting one if applicable.
     */
    @VisibleForTesting
    void evaluateOutputDir() {

        if (isDirectWrite()) {
            if (outputDir != null) {
                throw new IllegalStateException(
                    "Output directory should not be specified if direct-write (no reducers) is enabled");
            }
            return;
        }
        
        if (goLive) {
            if (outputDir == null) {
                outputDir = new Path(conf.get("hbase.search.mr.tmpdir", "/tmp"), "search-"
                        + UUID.randomUUID().toString());
                generatedOutputDir = true;
            }
        } else {
            if (outputDir == null) {
                throw new IllegalStateException("Must supply an output directory");
            }
        }
    }
    
    /**
     * Check if the output directory being used is an auto-generated temporary directory.
     */
    public boolean isGeneratedOutputDir() {
        return generatedOutputDir;
    }

    @VisibleForTesting
    void evaluateScan() {
        this.scan = new Scan();
        if (startRow != null) {
            scan.setStartRow(Bytes.toBytesBinary(startRow));
            LOG.debug("Starting row scan at " + startRow);
        }

        if (endRow != null) {
            scan.setStopRow(Bytes.toBytesBinary(endRow));
            LOG.debug("Stopping row scan at " + endRow);
        }

        if (startTime != null || endTime != null) {
            long scanStartTime = 0L;
            long scanEndTime = Long.MAX_VALUE;
            if (startTime != null) {
                scanStartTime = startTime;
                LOG.debug("Setting scan start of time range to " + startTime);
            }
            if (endTime != null) {
                scanEndTime = endTime;
                LOG.debug("Setting scan end of time range to " + endTime);
            }
            try {
                scan.setTimeRange(scanStartTime, scanEndTime);
            } catch (IOException e) {
                // In reality an IOE will never be thrown here
                throw new RuntimeException(e);
            }
        }
    }

    // Taken from org.apache.solr.hadoop.MapReduceIndexerTool
    @VisibleForTesting
    void evaluateGoLiveArgs() {
        if (zkHost == null && solrHomeDir == null) {
            throw new IllegalStateException("At least one of --zk-host or --solr-home-dir is required");
        }
        if (goLive && zkHost == null && shardUrls == null) {
            throw new IllegalStateException("--go-live requires that you also pass --shard-url or --zk-host");
        }

        if (zkHost != null && collection == null) {
            throw new IllegalStateException("--zk-host requires that you also pass --collection");
        }

        if (zkHost != null) {
            return;
            // verify structure of ZK directory later, to avoid checking run-time errors during parsing.
        } else if (shardUrls != null) {
            if (shardUrls.size() == 0) {
                throw new IllegalStateException("--shard-url requires at least one URL");
            }
        } else if (shards != null) {
            if (shards <= 0) {
                throw new IllegalStateException("--shards must be a positive number: " + shards);
            }
        } else {
            throw new IllegalStateException("You must specify one of the following (mutually exclusive) arguments: "
                    + "--zk-host or --shard-url or --shards");
        }

        if (shardUrls != null) {
            shards = shardUrls.size();
        }

    }

    // Taken from org.apache.solr.hadoop.MapReduceIndexerTool
    @VisibleForTesting
    private void evaluateNumReducers() {
        
        if (isDirectWrite()) {
            return;
        }
        
        if (shards <= 0) {
            throw new IllegalStateException("Illegal number of shards: " + shards);
        }
        if (fanout <= 1) {
            throw new IllegalStateException("Illegal fanout: " + fanout);
        }

        int reduceTaskCount;
        try {
            // MR1
            reduceTaskCount = new JobClient(conf).getClusterStatus().getMaxReduceTasks();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Cluster reports {} reduce slots", reducers);

        if (reducers == -2) {
            reduceTaskCount = shards;
        } else {
            reduceTaskCount = reducers;
        }
        reduceTaskCount = Math.max(reduceTaskCount, shards);

        if (reduceTaskCount != shards) {
            // Ensure fanout isn't misconfigured. fanout can't meaningfully be larger than what would be
            // required to merge all leaf shards in one single tree merge iteration into root shards
            fanout = Math.min(fanout, (int)ceilDivide(reduceTaskCount, shards));

            // Ensure invariant reducers == options.shards * (fanout ^ N) where N is an integer >= 1.
            // N is the number of mtree merge iterations.
            // This helps to evenly spread docs among root shards and simplifies the impl of the mtree merge algorithm.
            int s = shards;
            while (s < reducers) {
                s = s * fanout;
            }
            reduceTaskCount = s;
            assert reduceTaskCount % fanout == 0;
        }
        this.reducers = reduceTaskCount;
    }

    // same as IntMath.divide(p, q, RoundingMode.CEILING)
    private long ceilDivide(long p, long q) {
        long result = p / q;
        if (p % q != 0) {
            result++;
        }
        return result;
    }
}