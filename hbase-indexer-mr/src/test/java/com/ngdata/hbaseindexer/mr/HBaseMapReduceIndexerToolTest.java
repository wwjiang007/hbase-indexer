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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.util.net.NetUtils;
import com.ngdata.hbaseindexer.util.solr.SolrTestingUtility;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseMapReduceIndexerToolTest {

    private static final byte[] TEST_TABLE_NAME = Bytes.toBytes("record");
    private static final byte[] TEST_COLFAM_NAME = Bytes.toBytes("info");
    
    private static final HBaseTestingUtility HBASE_TEST_UTILITY = new HBaseTestingUtility();
    private static SolrTestingUtility SOLR_TEST_UTILITY;
    
    
    private static CloudSolrServer COLLECTION1;
    private static CloudSolrServer COLLECTION2;
    private static HBaseAdmin HBASE_ADMIN;

    private HTable recordTable;
    
    private HBaseIndexingOptions opts;
    
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        HBASE_TEST_UTILITY.startMiniCluster();
        startMrCluster();
        
        int zkClientPort = HBASE_TEST_UTILITY.getZkCluster().getClientPort();
        
        SOLR_TEST_UTILITY = new SolrTestingUtility(zkClientPort, NetUtils.getFreePort());
        SOLR_TEST_UTILITY.start();
        SOLR_TEST_UTILITY.uploadConfig("config1",
                Resources.toByteArray(Resources.getResource(HBaseMapReduceIndexerToolTest.class, "schema.xml")),
                Resources.toByteArray(Resources.getResource(HBaseMapReduceIndexerToolTest.class, "solrconfig.xml")));
        SOLR_TEST_UTILITY.createCore("collection1_core1", "collection1", "config1", 1);
        SOLR_TEST_UTILITY.createCore("collection2_core1", "collection2", "config1", 1);

        COLLECTION1 = new CloudSolrServer(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION1.setDefaultCollection("collection1");

        COLLECTION2 = new CloudSolrServer(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION2.setDefaultCollection("collection2");
        
    }
    
    private static void startMrCluster() throws Exception {
        // Handle compatibility between HBase 0.94 and HBase 0.95
        Method startMrClusterMethod = HBASE_TEST_UTILITY.getClass().getMethod("startMiniMapReduceCluster");
        
        if (Void.TYPE.equals(startMrClusterMethod.getReturnType())) {
            // HBase 0.94.x doesn't return a MR cluster, and puts the JobTracker
            // information directly in its own configuration
            HBASE_TEST_UTILITY.startMiniMapReduceCluster();
        } else {
            // HBase 0.95.x returns a MR cluster, and we have to manually
            // copy the job tracker address into our configuration
            MiniMRCluster mrCluster = (MiniMRCluster)startMrClusterMethod.invoke(HBASE_TEST_UTILITY);
            Configuration conf = HBASE_TEST_UTILITY.getConfiguration();
            conf.set("mapred.job.tracker", mrCluster.createJobConf().get("mapred.job.tracker"));
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        SOLR_TEST_UTILITY.stop();
        HBASE_ADMIN.close();
        HBASE_TEST_UTILITY.shutdownMiniMapReduceCluster();
        HBASE_TEST_UTILITY.shutdownMiniCluster();
    }
    
    @Before
    public void setUp() throws Exception {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TEST_TABLE_NAME);
        tableDescriptor.addFamily(new HColumnDescriptor(TEST_COLFAM_NAME));
        HBASE_ADMIN = new HBaseAdmin(HBASE_TEST_UTILITY.getConfiguration());
        HBASE_ADMIN.createTable(tableDescriptor);
        
        recordTable = new HTable(HBASE_TEST_UTILITY.getConfiguration(), TEST_TABLE_NAME);
        
        int zkPort = HBASE_TEST_UTILITY.getZkCluster().getClientPort();
        opts = new HBaseIndexingOptions(new Configuration());
        opts.zkHost = "127.0.0.1:" + zkPort + "/solr";
        opts.hbaseTableName = Bytes.toString(TEST_TABLE_NAME);
        opts.hbaseIndexerConfig = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.collection = "collection1";
        opts.shards = 1;
        opts.reducers = 1;
        opts.fanout = Integer.MAX_VALUE;
       
        opts.updateConflictResolver = RetainMostRecentUpdateConflictResolver.class.getName();
        opts.isVerbose = true;
    }
    
    @After
    public void tearDown() throws IOException, SolrServerException {
        HBASE_ADMIN.disableTable(TEST_TABLE_NAME);
        HBASE_ADMIN.deleteTable(TEST_TABLE_NAME);
        
        recordTable.close();
        
        COLLECTION1.deleteByQuery("*:*");
        COLLECTION1.commit();
        
        COLLECTION2.deleteByQuery("*:*");
        COLLECTION2.commit();
        
        // Be extra sure Solr is empty now
        QueryResponse response = COLLECTION1.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().isEmpty());
    }
    
    /**
     * Write String values to HBase. Direct string-to-bytes encoding is used for
     * writing all values to HBase. All values are stored in the TEST_COLFAM_NAME
     * column family.
     * 
     * 
     * @param row row key under which are to be stored
     * @param qualifiersAndValues map of column qualifiers to cell values
     */
    private void writeHBaseRecord(String row, Map<String,String> qualifiersAndValues) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        for (Entry<String, String> entry : qualifiersAndValues.entrySet()) {
            put.add(TEST_COLFAM_NAME, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        recordTable.put(put);
    }
    
    /**
     * Execute the indexing pipelines using the configured HBaseIndexingOptions.
     */
    private void executeIndexPipeline() throws Exception, SolrServerException, IOException {
        HBaseMapReduceIndexerTool indexerTool = new HBaseMapReduceIndexerTool();
        indexerTool.setConf(HBASE_TEST_UTILITY.getConfiguration());
        opts.evaluate();
        int exitCode = indexerTool.runIndexingJob(opts);
        
        assertEquals(0, exitCode);
        
        COLLECTION1.commit();
        COLLECTION2.commit();
    }
    
    /**
     * Execute a Solr query on COLLECTION1.
     * 
     * @param queryString Solr query string
     * @return list of results from Solr
     */
    private SolrDocumentList executeSolrQuery(String queryString) throws SolrServerException {
        return executeSolrQuery(COLLECTION1, queryString);
    }
    
    /**
     * Execute a Solr query on a specific collection.
     */
    private SolrDocumentList executeSolrQuery(CloudSolrServer collection, String queryString) throws SolrServerException {
        QueryResponse response = collection.query(new SolrQuery(queryString));
        return response.getResults();
    }
    
    @Test
    public void testIndexer_DirectWrite() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        opts.reducers = 0;
        
        executeIndexPipeline();
        
        assertEquals(1, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_Morphline() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        opts.reducers = 0;
        opts.hbaseIndexerConfig = substituteZkHost(new File("target/test-classes/morphline_indexer.xml"));
        opts.morphlineFile = new File("src/test/resources/extractHBaseCell.conf");
        opts.morphlineId = "morphline1";
        
        executeIndexPipeline();
        
        assertEquals(1, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_Morphline_With_DryRun() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        opts.isDryRun = true;
        opts.reducers = 0;
        opts.hbaseIndexerConfig = substituteZkHost(new File("target/test-classes/morphline_indexer.xml"));
        opts.morphlineFile = new File("src/test/resources/extractHBaseCell.conf");
        opts.morphlineId = "morphline1";
        
        executeIndexPipeline();
        
        assertEquals(0, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    private File substituteZkHost(File file) throws IOException {
      String str = Files.toString(file, Charsets.UTF_8);
      str = str.replace("_MYPATTERN_", SOLR_TEST_UTILITY.getZkConnectString());
      File tmp = File.createTempFile("tmpIndexer", ".xml");
      tmp.deleteOnExit();
      Files.write(str, tmp, Charsets.UTF_8);
      return tmp;
    }
    
    @Test
    public void testIndexer_AlternateCollection() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        opts.reducers = 0;
        opts.collection = "collection2";
        
        executeIndexPipeline();
        
        String solrQuery = "firstname_s:John lastname_s:Doe";
        
        assertTrue(executeSolrQuery(COLLECTION1, solrQuery).isEmpty());
        assertEquals(1, executeSolrQuery(COLLECTION2, solrQuery).size());
    }

   
    
    @Test
    public void testIndexer_StartRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        opts.reducers = 0;
        opts.startRow = "b";
        
        executeIndexPipeline();
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Aaron").isEmpty());
        
    }
    
    @Test
    public void testIndexer_EndRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        opts.reducers = 0;
        opts.endRow = "c";
        
        executeIndexPipeline();
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Carl").isEmpty());
    }
    
    @Test
    public void testIndexer_StartAndEndRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        opts.reducers = 0;
        opts.startRow = "b";
        opts.endRow = "c";
        
        executeIndexPipeline();
        
        assertEquals(1, executeSolrQuery("*:*").size());
        assertEquals(1, executeSolrQuery("firstname_s:Brian").size());
    }
    
    @Test
    public void testIndexer_StartTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));
        
        opts.reducers = 0;
        opts.startTimeString = "2";
        
        executeIndexPipeline();
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Early").isEmpty());
    }
    
    @Test
    public void testIndexer_EndTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));
        
        opts.reducers = 0;
        opts.endTimeString = "3";
        
        executeIndexPipeline();
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Late").isEmpty());
    }
    
    @Test
    public void testIndexer_StartAndEndTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));
        
        opts.reducers = 0;
        opts.startTimeString = "2";
        opts.endTimeString = "3";
        
        executeIndexPipeline();
        
        assertEquals(1, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Early").isEmpty());
        assertEquals(1, executeSolrQuery("firstname_s:Ontime").size());
        assertTrue(executeSolrQuery("firstname_s:Late").isEmpty());
    }
   
    

    @Test
    public void testIndexer_ToHdfs() throws Exception {
        
        writeHBaseRecord("row1", ImmutableMap.of("firstname", "John", "lastname", "Doe"));
        writeHBaseRecord("row2", ImmutableMap.of("firstname", "Jane", "lastname", "Doe"));

        opts.reducers = 1;
        opts.outputDir = new Path("/solroutput");
        executeIndexPipeline();
        
        // TODO Validate output
    }

}
