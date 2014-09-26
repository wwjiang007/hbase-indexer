package com.ngdata.hbaseindexer.master;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.mockito.internal.verification.VerificationModeFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class BatchStateUpdaterTest {
    WriteableIndexerModel model;
    JobClient jobClient;
    List<IndexerDefinition> indexerDefinitionList;
    ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5);


    @Before
    public void setUp() throws Exception {
        indexerDefinitionList = Lists.newArrayList();
        model = mock(WriteableIndexerModel.class);
        when(model.getIndexers()).thenReturn(indexerDefinitionList);

        jobClient = mock(JobClient.class);
    }

    @After
    public void tearDown() throws Exception {
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
        String jobId = "job_201407251005_0815";
        createDefinition("mytest", jobId);
        createJob(jobId, JobStatus.SUCCEEDED);

        checkAllIndexes();

        verify(model, VerificationModeFactory.atLeastOnce()).updateIndexerInternal(any(IndexerDefinition.class));
    }

    @Test
    public void testRun_Empty() throws Exception {
        createDefinition("mytest", null);
        checkAllIndexes();
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));
    }

    @Test
    public void testRun_Running() throws Exception {
        String jobId = "job_201407251005_0815";
        createDefinition("mytest", jobId);
        RunningJob job = createJob(jobId, JobStatus.RUNNING);

        when(job.getJobState()).thenReturn(JobStatus.RUNNING);

        Assert.assertEquals(0, executorService.getQueue().size());
        checkAllIndexes();

        Assert.assertEquals(1, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(1)).getIndexer(anyString());
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));
        Thread.sleep(60);
        Assert.assertEquals(1, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(2)).getIndexer(anyString());
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));


        when(job.getJobState()).thenReturn(JobStatus.SUCCEEDED);
        Thread.sleep(60);
        Assert.assertEquals(0, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(3)).getIndexer(anyString());
        verify(model, VerificationModeFactory.times(1)).updateIndexerInternal(any(IndexerDefinition.class));
    }

    @Test

    public void testRun_MultiIndex() throws Exception {
        String jobId1 = "job_201407251005_0001";
        String jobId2 = "job_201407251005_0002";
        createDefinition("mytest1", jobId1);
        RunningJob job1 = createJob(jobId1, JobStatus.RUNNING);
        createDefinition("mytest2", jobId2);
        RunningJob job2 = createJob(jobId2, JobStatus.RUNNING);

        Assert.assertEquals(0, executorService.getQueue().size());
        checkAllIndexes();

        Assert.assertEquals(2, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));
        Thread.sleep(60);
        Assert.assertEquals(2, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(0)).updateIndexerInternal(any(IndexerDefinition.class));

        when(job1.getJobState()).thenReturn(JobStatus.SUCCEEDED);
        Thread.sleep(60);
        Assert.assertEquals(1, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(1)).updateIndexerInternal(any(IndexerDefinition.class));

        when(job2.getJobState()).thenReturn(JobStatus.SUCCEEDED);
        Thread.sleep(60);
        Assert.assertEquals(0, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(2)).updateIndexerInternal(any(IndexerDefinition.class));

        Thread.sleep(60);
        Assert.assertEquals(0, executorService.getQueue().size());
        verify(model, VerificationModeFactory.times(2)).updateIndexerInternal(any(IndexerDefinition.class));
    }

    private RunningJob createJob(String jobId, int status) throws IOException {
        RunningJob job = mock(RunningJob.class);
        when(job.getJobState()).thenReturn(status);
        when(jobClient.getJob(JobID.forName(jobId))).thenReturn(job);
        return job;
    }

    private IndexerDefinition createDefinition(String name, String jobId) throws Exception {
        IndexerDefinition def = mock(IndexerDefinition.class);
        when(def.getName()).thenReturn(name);
        when(def.getLifecycleState()).thenReturn(IndexerDefinition.LifecycleState.ACTIVE);
        when(def.getIncrementalIndexingState()).thenReturn(IndexerDefinition.IncrementalIndexingState.DEFAULT);

        when(model.getIndexer(name)).thenReturn(def);
        when(model.getFreshIndexer(name)).thenReturn(def);
        indexerDefinitionList.add(def);

        if (jobId != null) {
            Map<String, String> jobTrackingUrls = Maps.newHashMap();
            jobTrackingUrls.put(jobId, "no-matter");
            BatchBuildInfo bbi = mock(BatchBuildInfo.class);
            when(bbi.getMapReduceJobTrackingUrls()).thenReturn(jobTrackingUrls);

            when(def.getActiveBatchBuildInfo()).thenReturn(bbi);
        }
        return def;
    }

    private void checkAllIndexes() {
        for (IndexerDefinition def : indexerDefinitionList) {
            BatchStateUpdater batchStateUpdater = new BatchStateUpdater(def.getName(), model, jobClient, executorService, 50);
            batchStateUpdater.run();
        }
    }


}
