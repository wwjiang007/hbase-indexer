/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.google.common.collect.Lists;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.impl.SepHBaseSchema.RecordCf;
import com.ngdata.sep.impl.SepHBaseSchema.RecordColumn;
import com.ngdata.zookeeper.ZooKeeperItf;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SepEventSlaveTest {
    
    private static final long SUBSCRIPTION_TIMESTAMP = 100000;

    private EventListener eventListener;
    private ZooKeeperItf zkItf;
    private SepEventSlave eventSlave;

    @Before
    public void setUp() throws IOException, InterruptedException, KeeperException {
        eventListener = mock(EventListener.class);
        zkItf = mock(ZooKeeperItf.class);
        eventSlave = new SepEventSlave("subscriptionId", SUBSCRIPTION_TIMESTAMP, eventListener, 1, "localhost", zkItf,
                HBaseConfiguration.create());
    }

    @After
    public void tearDown() {
        eventSlave.stop();
    }

    private HLog.Entry[] createHlogEntries(byte[] tableName, byte[] rowKey, byte[] columnFamily, byte[] qualifier,
            byte[] data) {
        return createHlogEntries(tableName, rowKey, columnFamily, qualifier, data, SUBSCRIPTION_TIMESTAMP +1);
    }
    
    // Overload of createHlogEntries to allow specifying a write time in the HLog
    private HLog.Entry[] createHlogEntries(byte[] tableName, byte[] rowKey, byte[] columnFamily, byte[] qualifier,
            byte[] data, long writeTime) {
        KeyValue keyValue = new KeyValue(rowKey, columnFamily, qualifier, data);
        HLog.Entry entry = mock(HLog.Entry.class, Mockito.RETURNS_DEEP_STUBS);
        when(entry.getEdit().getKeyValues()).thenReturn(Lists.newArrayList(keyValue));
        when(entry.getKey().getTablename()).thenReturn(tableName);
        when(entry.getKey().getWriteTime()).thenReturn(writeTime);
        return new HLog.Entry[] { entry };
    }

    @Test
    public void testReplicateLogEntries() throws IOException {

        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadData = Bytes.toBytes("payload");

        HLog.Entry[] hlogEntries = createHlogEntries(SepHBaseSchema.RECORD_TABLE, rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadData);

        eventSlave.replicateLogEntries(hlogEntries);

        verify(eventListener).processMessage(aryEq(rowKey), aryEq(payloadData));
    }

    @Test
    public void testReplicateLogEntries_NotForRecordTable() throws IOException {

        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadData = Bytes.toBytes("payload");

        HLog.Entry[] hlogEntries = createHlogEntries(Bytes.toBytes("NotRecordTable"), rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadData);

        eventSlave.replicateLogEntries(hlogEntries);

        // Event listener shouldn't be touched as the event wasn't on the record table
        verify(eventListener, never()).processMessage(any(byte[].class), any(byte[].class));
    }
    
    @Test
    public void testReplicateLogEntries_EntryTimestampBeforeSubscriptionTimestamp() throws IOException {
        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadDataBeforeTimestamp = Bytes.toBytes("payloadBeforeTimestamp");
        byte[] payloadDataOnTimestamp = Bytes.toBytes("payloadOnTimestamp");
        byte[] payloadDataAfterTimestamp = Bytes.toBytes("payloadAfterTimestamp");

        HLog.Entry[] hlogEntriesBeforeTimestamp = createHlogEntries(SepHBaseSchema.RECORD_TABLE, rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadDataBeforeTimestamp, SUBSCRIPTION_TIMESTAMP - 1);
        HLog.Entry[] hlogEntriesOnTimestamp = createHlogEntries(SepHBaseSchema.RECORD_TABLE, rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadDataOnTimestamp, SUBSCRIPTION_TIMESTAMP);
        HLog.Entry[] hlogEntriesAfterTimestamp = createHlogEntries(SepHBaseSchema.RECORD_TABLE, rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadDataAfterTimestamp, SUBSCRIPTION_TIMESTAMP + 1);

        eventSlave.replicateLogEntries(hlogEntriesBeforeTimestamp);
        eventSlave.replicateLogEntries(hlogEntriesOnTimestamp);
        eventSlave.replicateLogEntries(hlogEntriesAfterTimestamp);

        // Event should be published for data on or after the subscription timestamp, but not before
        verify(eventListener, never()).processMessage(aryEq(rowKey), aryEq(payloadDataBeforeTimestamp));
        verify(eventListener, times(1)).processMessage(aryEq(rowKey), aryEq(payloadDataOnTimestamp));
        verify(eventListener, times(1)).processMessage(aryEq(rowKey), aryEq(payloadDataAfterTimestamp));
    }
}
