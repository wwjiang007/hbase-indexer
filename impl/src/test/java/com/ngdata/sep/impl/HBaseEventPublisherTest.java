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

import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.ngdata.sep.impl.SepHBaseSchema.RecordCf;
import com.ngdata.sep.impl.SepHBaseSchema.RecordColumn;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HBaseEventPublisherTest {

    private HTableInterface recordTable;
    private HBaseEventPublisher eventPublisher;

    @Before
    public void setUp() {
        recordTable = mock(HTableInterface.class);
        eventPublisher = new HBaseEventPublisher(recordTable);
    }

    @Test
    public void testProcessMessage_RowInRepository() throws IOException {
        byte[] messageRow = Bytes.toBytes("row-id");
        byte[] messagePayload = Bytes.toBytes("payload");
        
        Put expectedPut = new Put(messageRow);
        expectedPut.add(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, messagePayload);
        
        when(recordTable.checkAndPut(aryEq(messageRow), aryEq(RecordCf.DATA.bytes), aryEq(RecordColumn.DELETED.bytes),
                aryEq(Bytes.toBytes(false)), any(Put.class))).thenReturn(true);
        
        boolean publishResult = eventPublisher.publishMessage(messageRow, messagePayload);
        assertTrue(publishResult);
    }
    
    
    @Test
    public void testProcessMessage_RowNotInRepository() throws IOException {
        byte[] messageRow = Bytes.toBytes("row-id");
        byte[] messagePayload = Bytes.toBytes("payload");
        
        Put expectedPut = new Put(messageRow);
        expectedPut.add(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, messagePayload);
        
        when(recordTable.checkAndPut(aryEq(messageRow), aryEq(RecordCf.DATA.bytes), aryEq(RecordColumn.DELETED.bytes),
                aryEq(Bytes.toBytes(false)), any(Put.class))).thenReturn(false);
        
        boolean publishResult = eventPublisher.publishMessage(messageRow, messagePayload);
        assertTrue(!publishResult);
    }

}
