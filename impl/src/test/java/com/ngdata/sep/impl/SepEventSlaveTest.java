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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.google.common.collect.Lists;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
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

    private HLog.Entry createHlogEntry(byte[] tableName, KeyValue... keyValues) {
        return createHlogEntry(tableName, SUBSCRIPTION_TIMESTAMP + 1, keyValues);
    }

    private HLog.Entry createHlogEntry(byte[] tableName, long writeTime, KeyValue... keyValues) {
        HLog.Entry entry = mock(HLog.Entry.class, Mockito.RETURNS_DEEP_STUBS);
        when(entry.getEdit().getKeyValues()).thenReturn(Lists.newArrayList(keyValues));
        when(entry.getKey().getTablename()).thenReturn(tableName);
        when(entry.getKey().getWriteTime()).thenReturn(writeTime);
        return entry;
    }

    @Test
    public void testReplicateLogEntries() throws IOException {

        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadData = Bytes.toBytes("payload");

        HLog.Entry hlogEntry = createHlogEntry(SepHBaseSchema.RECORD_TABLE, new KeyValue(rowKey, RecordCf.DATA.bytes,
                RecordColumn.PAYLOAD.bytes, payloadData));

        eventSlave.replicateLogEntries(new HLog.Entry[] { hlogEntry });

        SepEvent expectedSepEvent = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKey,
                hlogEntry.getEdit().getKeyValues(), payloadData);

        verify(eventListener).processEvent(expectedSepEvent);
    }

    @Test
    public void testReplicateLogEntries_EntryTimestampBeforeSubscriptionTimestamp() throws IOException {
        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadDataBeforeTimestamp = Bytes.toBytes("payloadBeforeTimestamp");
        byte[] payloadDataOnTimestamp = Bytes.toBytes("payloadOnTimestamp");
        byte[] payloadDataAfterTimestamp = Bytes.toBytes("payloadAfterTimestamp");

        HLog.Entry hlogEntryBeforeTimestamp = createHlogEntry(SepHBaseSchema.RECORD_TABLE, SUBSCRIPTION_TIMESTAMP - 1,
                new KeyValue(rowKey, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, payloadDataBeforeTimestamp));
        HLog.Entry hlogEntryOnTimestamp = createHlogEntry(SepHBaseSchema.RECORD_TABLE, SUBSCRIPTION_TIMESTAMP,
                new KeyValue(rowKey, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, payloadDataOnTimestamp));
        HLog.Entry hlogEntryAfterTimestamp = createHlogEntry(SepHBaseSchema.RECORD_TABLE, SUBSCRIPTION_TIMESTAMP + 1,
                new KeyValue(rowKey, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, payloadDataAfterTimestamp));

        eventSlave.replicateLogEntries(new HLog.Entry[] { hlogEntryBeforeTimestamp });
        eventSlave.replicateLogEntries(new HLog.Entry[] { hlogEntryOnTimestamp });
        eventSlave.replicateLogEntries(new HLog.Entry[] { hlogEntryAfterTimestamp });

        SepEvent expectedEventOnTimestamp = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKey,
                hlogEntryOnTimestamp.getEdit().getKeyValues(), payloadDataOnTimestamp);
        SepEvent expectedEventAfterTimestamp = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKey,
                hlogEntryAfterTimestamp.getEdit().getKeyValues(), payloadDataAfterTimestamp);

        // Event should be published for data on or after the subscription timestamp, but not before
        verify(eventListener, times(1)).processEvent(expectedEventOnTimestamp);
        verify(eventListener, times(1)).processEvent(expectedEventAfterTimestamp);
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testReplicateLogEntries_MultipleKeyValuesForSingleRow() throws Exception {
        byte[] rowKey = Bytes.toBytes("rowKey");

        KeyValue kvA = new KeyValue(rowKey, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, Bytes.toBytes("A"));
        KeyValue kvB = new KeyValue(rowKey, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, Bytes.toBytes("B"));

        HLog.Entry entry = createHlogEntry(SepHBaseSchema.RECORD_TABLE, kvA, kvB);

        eventSlave.replicateLogEntries(new HLog.Entry[] { entry });

        // We should get the first payload in our event (and the second one will be ignored, although the KeyValue will
        // be present in the event
        SepEvent expectedEvent = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKey, Lists.newArrayList(kvA, kvB),
                Bytes.toBytes("A"));

        verify(eventListener).processEvent(expectedEvent);
    }

    // A multi-put could result in updates to multiple rows being included in a single WALEdit.
    // In this case, the KeyValues for separate rows should be separated and result in separate events.
    @Test
    public void testReplicateLogEntries_SingleWALEditForMultipleRows() throws IOException {

        byte[] rowKeyA = Bytes.toBytes("A");
        byte[] rowKeyB = Bytes.toBytes("B");
        byte[] data = Bytes.toBytes("data");

        KeyValue kvA = new KeyValue(rowKeyA, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, data);
        KeyValue kvB = new KeyValue(rowKeyB, RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, data);

        HLog.Entry entry = createHlogEntry(SepHBaseSchema.RECORD_TABLE, kvA, kvB);

        eventSlave.replicateLogEntries(new HLog.Entry[] { entry });

        SepEvent expectedEventA = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKeyA, Lists.newArrayList(kvA),
                Bytes.toBytes("data"));
        SepEvent expectedEventB = new SepEvent(SepHBaseSchema.RECORD_TABLE, rowKeyB, Lists.newArrayList(kvB),
                Bytes.toBytes("data"));

        verify(eventListener).processEvent(expectedEventA);
        verify(eventListener).processEvent(expectedEventB);
    }
}
