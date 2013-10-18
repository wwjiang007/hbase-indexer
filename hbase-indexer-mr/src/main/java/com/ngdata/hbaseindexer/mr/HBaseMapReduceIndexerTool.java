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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.hadoop.ForkedMapReduceIndexerTool;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top-level tool for running MapReduce-based indexing pipelines over HBase tables.
 */
public class HBaseMapReduceIndexerTool extends Configured implements Tool {
    
    private static final Logger LOG = LoggerFactory.getLogger(ForkedMapReduceIndexerTool.class);
    
    public static void main(String[] args) throws Exception {
        
        int res = ToolRunner.run(new Configuration(), new HBaseMapReduceIndexerTool(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        
        HBaseIndexingOptions hbaseIndexingOpts = new HBaseIndexingOptions(getConf());
        Integer exitCode = new HBaseIndexerArgumentParser().parseArgs(args, getConf(), hbaseIndexingOpts);
        if (exitCode != null) {
          return exitCode;
        }
        
        if (hbaseIndexingOpts.isDryRun) {
            return new IndexerDryRun(hbaseIndexingOpts, getConf(), System.out).run();
        } else {
            return runIndexingJob(hbaseIndexingOpts);
        }
    }
    
    public int runIndexingJob(HBaseIndexingOptions hbaseIndexingOpts) throws Exception {
        
        Configuration conf = getConf();

        IndexingSpecification indexingSpec = hbaseIndexingOpts.getIndexingSpecification();
        
        conf.set(HBaseIndexerMapper.INDEX_CONFIGURATION_CONF_KEY, indexingSpec.getIndexConfigXml());
        conf.set(HBaseIndexerMapper.INDEX_NAME_CONF_KEY, indexingSpec.getIndexerName());
        conf.set(HBaseIndexerMapper.TABLE_NAME_CONF_KEY, indexingSpec.getTableName());
        HBaseIndexerMapper.configureIndexConnectionParams(conf, indexingSpec.getIndexConnectionParams());
        
        conf.setBoolean(HBaseIndexerMapper.INDEX_DIRECT_WRITE_CONF_KEY, hbaseIndexingOpts.isDirectWrite());
        
        Job job = Job.getInstance(getConf());
        job.setJarByClass(HBaseIndexerMapper.class);
        job.setUserClassesTakesPrecedence(true);
        
        TableMapReduceUtil.initTableMapperJob(
                                    indexingSpec.getTableName(),
                                    hbaseIndexingOpts.getScan(),
                                    HBaseIndexerMapper.class,
                                    Text.class,
                                    SolrInputDocumentWritable.class,
                                    job);
        
        if (hbaseIndexingOpts.isDirectWrite()) {
            int exitCode = runDirectWriteIndexingJob(job, getConf(), hbaseIndexingOpts.isVerbose);
            if (exitCode == 0) {
                CloudSolrServer solrServer = new CloudSolrServer(hbaseIndexingOpts.zkHost);
                solrServer.setDefaultCollection(hbaseIndexingOpts.collection);
                solrServer.commit(false, false);
                solrServer.shutdown();
            }
            return exitCode;
        } else {
            FileSystem fileSystem = FileSystem.get(getConf());
            int exitCode = ForkedMapReduceIndexerTool.runIndexingPipeline(
                                            job, getConf(), hbaseIndexingOpts.asOptions(),
                                            System.currentTimeMillis(),
                                            fileSystem,
                                            null, -1, // File-based parameters
                                            -1, // num mappers, only of importance for file-based indexing
                                            hbaseIndexingOpts.reducers
                                            );
            

            if (hbaseIndexingOpts.isGeneratedOutputDir()) {
                LOG.info("Deleting generated output directory " + hbaseIndexingOpts.outputDir);
                fileSystem.delete(hbaseIndexingOpts.outputDir, true);
            }
            return exitCode;
        }
        
    }

    /**
     * Write a map-only MR job that writes index documents directly to a live Solr instance.
     * 
     * @param job configured job for creating SolrInputDocuments
     * @param conf job configuration
     * @param verbose run in verbose mode
     * @return exit code, 0 is successful execution
     */
    private int runDirectWriteIndexingJob(Job job, Configuration conf, boolean verbose)
                throws ClassNotFoundException, IOException, InterruptedException {
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        return ForkedMapReduceIndexerTool.waitForCompletion(job, verbose) ? 0 : 1;
    }

}
