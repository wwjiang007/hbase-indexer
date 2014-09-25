package com.ngdata.hbaseindexer.master;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BatchStateUpdaterTest {
    BatchStateUpdater batchStateUpdater;
    WriteableIndexerModel model;
    JobClient jobClient;
    ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);
    String jobId= "job_201407251005_0815";


    @Before
    public void setUp() throws Exception {
        Map<String,String> jobTrackingUrls = Maps.newHashMap();
        jobTrackingUrls.put(jobId, "no-matter");
        BatchBuildInfo bbi =   mock(BatchBuildInfo.class);
        when(bbi.getMapReduceJobTrackingUrls()).thenReturn(jobTrackingUrls);
        IndexerDefinition def = mock(IndexerDefinition.class);
        when(def.getActiveBatchBuildInfo()).thenReturn(bbi);
        when(def.getName()).thenReturn("mytest");
        when(def.getLifecycleState()).thenReturn(IndexerDefinition.LifecycleState.ACTIVE);
        when(def.getIncrementalIndexingState()).thenReturn(IndexerDefinition.IncrementalIndexingState.DEFAULT);

        model = mock(WriteableIndexerModel.class);
        when(model.getIndexers()).thenReturn(Lists.newArrayList(def));
        when(model.getIndexer("mytest")).thenReturn(def);
        when(model.getFreshIndexer(anyString())).thenReturn(def);

        jobClient = mock(JobClient.class);
        RunningJob job = mock(RunningJob.class);
        when(job.getJobState()).thenReturn(JobStatus.SUCCEEDED);
        when(jobClient.getJob(JobID.forName(jobId))).thenReturn(job);

        batchStateUpdater = new BatchStateUpdater(model, jobClient, executorService, 50);
    }

    @After
    public void tearDown () throws Exception {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void testRun() throws Exception {
        batchStateUpdater.run();
        verify(model, VerificationModeFactory.atLeastOnce()).updateIndexerInternal(any(IndexerDefinition.class));
    }

    @Test
    public void testRun_Empty() throws Exception {
        IndexerDefinition def = model.getIndexer("mytest");
        when(def.getActiveBatchBuildInfo()).thenReturn(null);
        batchStateUpdater.run();
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));
    }

    @Test
    public void testRun_Running() throws Exception {
        RunningJob job = jobClient.getJob(JobID.forName(jobId));
        when(job.getJobState()).thenReturn(JobStatus.RUNNING);

        Assert.assertEquals(0,executorService.getQueue().size());
        batchStateUpdater.run();
        Assert.assertEquals(1,executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(1)).getIndexers();
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));
        Thread.sleep(60);
        Assert.assertEquals(1,executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(2)).getIndexers();
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));


        when(job.getJobState()).thenReturn(JobStatus.SUCCEEDED);
        Thread.sleep(60);
        Assert.assertEquals(0,executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(3)).getIndexers();
        verify(model, VerificationModeFactory.times(1)).updateIndexerInternal(any(IndexerDefinition.class));
    }
}
