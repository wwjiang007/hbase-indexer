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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseIndexingOptionsTest {

    private Configuration conf;
    private HBaseIndexingOptions opts;
    
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
        
        Path outputPath = opts.outputDir;
        assertEquals(new Path("/tmp"), outputPath.getParent());
        assertTrue(opts.isGeneratedOutputDir());
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
    

}
