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

import java.io.IOException;

import com.ngdata.sep.EventPublisher;
import com.ngdata.sep.impl.SepHBaseSchema.RecordCf;
import com.ngdata.sep.impl.SepHBaseSchema.RecordColumn;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Publishes side-effect events directly to the HBase record table.
 */
public class HBaseEventPublisher implements EventPublisher {

    private static final byte[] FALSE_BYTES = Bytes.toBytes(false);

    private HTableInterface recordTable;

    public HBaseEventPublisher(HTableInterface recordTable) {
        this.recordTable = recordTable;
    }

    @Override
    public boolean publishMessage(byte[] row, byte[] payload) throws IOException {
        Put messagePut = new Put(row);
        messagePut.add(RecordCf.DATA.bytes, RecordColumn.PAYLOAD.bytes, 1L, payload);
        return recordTable.checkAndPut(row, RecordCf.DATA.bytes, RecordColumn.DELETED.bytes, FALSE_BYTES,
                messagePut);
    }

}
