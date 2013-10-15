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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseIndexingOptionsTest {
    
    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;

    private Configuration conf;
    private HBaseIndexingOptions opts;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "hbaseindexer.zktest");
        ZK_CLIENT_PORT = getFreePort();

        ZK_CLUSTER = new MiniZooKeeperCluster();
        ZK_CLUSTER.setDefaultClientPort(ZK_CLIENT_PORT);
        ZK_CLUSTER.startup(ZK_DIR);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (ZK_CLUSTER != null) {
            ZK_CLUSTER.shutdown();
        }
        FileUtils.deleteDirectory(ZK_DIR);
    }
    
    private static int getFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Error finding a free port", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing ServerSocket used to detect a free port.", e);
                }
            }
        }
    }
    
    @Before
    public void setUp() {
        conf = new Configuration();
        opts = new HBaseIndexingOptions(conf);
    }
    
    
    @Test
    public void testIsDirectWrite_True() {
        opts.reducers = 0;
        
        assertTrue(opts.isDirectWrite());
    }
    
    
    @Test
    public void testIsDirectWrite_False() {
        opts.reducers = 1;
        assertFalse(opts.isDirectWrite());
        
        // Different negative values have different meanings in core search MR job
        opts.reducers = -1;
        assertFalse(opts.isDirectWrite());
        
        opts.reducers = -2;
        assertFalse(opts.isDirectWrite());
    }
    
    @Test
    public void testEvaluateOutputDir_DirectWrite() {
        opts.outputDir = null;
        opts.reducers = 0;
        
        // Should have no effect
        opts.evaluateOutputDir();
        
        assertNull(opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }
    
    @Test
    public void testEvaluateOutputDir_GoLive() {
        opts.outputDir = null;
        opts.reducers = 2;
        opts.goLive = true;
        
        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/tmp"), outputPath.getParent());
        assertTrue(opts.isGeneratedOutputDir());
    }
    
    @Test
    public void testEvaluateOutputDir_GoLive_UserDefinedOutputDir() {
        opts.outputDir = new Path("/outputdir");
        opts.reducers = 2;
        opts.goLive = true;
        
        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/outputdir"), outputPath);
        assertFalse(opts.isGeneratedOutputDir());
    }
    
    @Test
    public void testEvaluateOutputDir_GoLive_AlternateTempDirViaConfig() {
        opts.outputDir = null;
        opts.reducers = 2;
        opts.goLive = true;
        conf.set("hbase.search.mr.tmpdir", "/othertmp");
        
        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/othertmp"), outputPath.getParent());
        assertTrue(opts.isGeneratedOutputDir());
    }
    
    @Test
    public void testEvaluateOutputDir_WriteToFile() {
        opts.outputDir = new Path("/output/path");
        opts.reducers = 2;
        
        // Should have no effect
        opts.evaluateOutputDir();
        
        assertEquals(new Path("/output/path"), opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateOutputDir_NoOutputDirButNotDirectWriteMode() {
        opts.outputDir = null;
        opts.reducers = 1;
        
        opts.evaluateOutputDir();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateOutputDir_OutputDirSetAlongWithDirectWriteMode() {
        opts.outputDir = new Path("/some/path");
        opts.reducers = 0;
        
        opts.evaluateOutputDir();
    }
    
    @Test
    public void testEvaluateScan_StartRowDefined() {
        opts.startRow = "starthere";
        opts.evaluateScan();
        assertArrayEquals(Bytes.toBytes("starthere"), opts.getScan().getStartRow());
    }
    
    @Test
    public void testEvaluateScan_EndRowDefined() {
        opts.endRow = "endhere";
        opts.evaluateScan();
        assertArrayEquals(Bytes.toBytes("endhere"), opts.getScan().getStopRow());
    }
    
    @Test
    public void testEvaluateScan_StartTimeDefined() {
        opts.startTime = 220777L;
        opts.evaluateScan();
        assertEquals(220777L, opts.getScan().getTimeRange().getMin());
    }
    
    @Test
    public void testEvaluateScan_EndTimeDefined() {
        opts.endTime = 220777L;
        opts.evaluateScan();
        assertEquals(220777L, opts.getScan().getTimeRange().getMax());
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_NoZkHostOrSolrHomeDir() {
        opts.solrHomeDir = null;
        opts.zkHost = null;
        
        opts.evaluateGoLiveArgs();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_GoLiveButNoZkAndNoShards() {
        opts.goLive = true;
        opts.zkHost = null;
        opts.shardUrls = null;
        
        opts.evaluateGoLiveArgs();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_ZkHostButNoCollection() {
        opts.zkHost = "myzkhost";
        opts.collection = null;
        
        opts.evaluateGoLiveArgs();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_EmptyShardUrls() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shardUrls = ImmutableList.of();
        
        opts.evaluateGoLiveArgs();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_ZeroShards() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shards = 0;
        
        opts.evaluateGoLiveArgs();
    }
    
    @Test
    public void testEvaluateGoLiveArgs_AutoSetShardCount() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shardUrls = ImmutableList.<List<String>>of(ImmutableList.of("shard_a"), ImmutableList.of("shard_b"));
        opts.shards = null;
        
        opts.evaluateGoLiveArgs();
        
        assertEquals(2, (int)opts.shards);
        
    }
    
    
    @Test
    public void testEvaluateIndexingSpecification_AllFromZooKeeper() throws Exception {
        
        ZooKeeperItf zk = ZkUtil.connect("localhost:" + ZK_CLIENT_PORT, 5000);
        WriteableIndexerModel indexerModel = new IndexerModelImpl(zk, "/ngdata/hbaseindexer");

        // Create an indexer -- verify INDEXER_ADDED event
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("userindexer")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        indexerModel.addIndexer(indexerDef);
        
        opts.indexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.indexerName = "userindexer";
        
        opts.evaluateIndexingSpecification();
        indexerModel.deleteIndexerInternal("userindexer");
        
        IndexingSpecification expectedSpec = new IndexingSpecification(
                            "record", "userindexer",
                            Resources.toString(Resources.getResource(getClass(), "user_indexer.xml"), Charsets.UTF_8),
                            ImmutableMap.of(
                                    "solr.zk", "myZkHost/solr",
                                    "solr.collection", "mycollection"));
        
        
        
        
        assertEquals(expectedSpec, opts.getIndexingSpecification());
    }
    
    @Test
    public void testEvaluateIndexingSpecification_AllFromCmdline() throws Exception {
        opts.indexerZkHost = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfig = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";
        
        opts.evaluateIndexingSpecification();
        
        IndexingSpecification expectedSpec = new IndexingSpecification(
                            "mytable", HBaseIndexingOptions.DEFAULT_INDEXER_NAME,
                            Resources.toString(Resources.getResource(getClass(), "user_indexer.xml"), Charsets.UTF_8),
                            ImmutableMap.of(
                                    "solr.zk", "myZkHost/solr",
                                    "solr.collection", "mycollection"));
        
        assertEquals(expectedSpec, opts.getIndexingSpecification());
        
    }
    
    @Test
    public void testEvaluateIndexingSpecification_TableNameFromXmlFile() throws Exception {
        opts.indexerZkHost = null;
        opts.hbaseIndexerConfig = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";
        
        opts.evaluateIndexingSpecification();
        
        IndexingSpecification expectedSpec = new IndexingSpecification(
                "record", HBaseIndexingOptions.DEFAULT_INDEXER_NAME,
                Resources.toString(Resources.getResource(getClass(), "user_indexer.xml"), Charsets.UTF_8),
                ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"));
        
        assertEquals(expectedSpec, opts.getIndexingSpecification());
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoIndexXmlSpecified() throws Exception {
        opts.indexerZkHost = null;
        opts.hbaseIndexerConfig = null;
        opts.hbaseTableName = "mytable";
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";
        
        opts.evaluateIndexingSpecification();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoZkHostSpecified() throws Exception {
        opts.indexerZkHost = null;
        opts.zkHost = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfig = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.collection = "mycollection";
        
        opts.evaluateIndexingSpecification();
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoCollectionSpecified() throws Exception {
        opts.indexerZkHost = null;
        opts.collection = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfig = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";
        
        opts.evaluateIndexingSpecification();
    }
    
    @Test
    public void testEvaluateIndexingSpecification_CombinationOfCmdlineAndZk() throws Exception {
        ZooKeeperItf zk = ZkUtil.connect("localhost:" + ZK_CLIENT_PORT, 5000);
        WriteableIndexerModel indexerModel = new IndexerModelImpl(zk, "/ngdata/hbaseindexer");

        // Create an indexer -- verify INDEXER_ADDED event
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("userindexer")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        indexerModel.addIndexer(indexerDef);
        
        opts.indexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.indexerName = "userindexer";
        opts.hbaseTableName = "mytable";
        opts.zkHost = "myOtherZkHost/solr";
        
        opts.evaluateIndexingSpecification();
        
        indexerModel.deleteIndexerInternal("userindexer");
        
        IndexingSpecification expectedSpec = new IndexingSpecification(
                "mytable", "userindexer",
                Resources.toString(Resources.getResource(getClass(), "user_indexer.xml"), Charsets.UTF_8),
                ImmutableMap.of(
                        "solr.zk", "myOtherZkHost/solr",
                        "solr.collection", "mycollection"));
        
        assertEquals(expectedSpec, opts.getIndexingSpecification());
        
    }

}
