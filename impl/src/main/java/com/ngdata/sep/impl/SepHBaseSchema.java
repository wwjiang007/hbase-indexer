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

import org.apache.hadoop.hbase.util.Bytes;

public class SepHBaseSchema {
    
    public static final byte[] RECORD_TABLE = Bytes.toBytes("record");
    
    /**
     * Column families in the record table.
     */
    public static enum RecordCf {
        DATA("data"); // The actual data fields and system fields of records are stored in the same column family

        public final byte[] bytes;
        public final String name;

        RecordCf(String name) {
            this.name = name;
            this.bytes = Bytes.toBytes(name);
        }
    }
    
    /**
     * Columns in the record table.
     */
    public static enum RecordColumn {
        /** occ = optimistic concurrency control (a version counter) */
        OCC("occ"),
        VERSION("version"),
        DELETED("deleted"),
        NON_VERSIONED_RT_ID("nv-rt"),
        NON_VERSIONED_RT_VERSION("nv-rtv"),
        VERSIONED_RT_ID("v-rt"),
        VERSIONED_RT_VERSION("v-rtv"),
        VERSIONED_MUTABLE_RT_ID("vm-rt"),
        VERSIONED_MUTABLE_RT_VERSION("vm-rtv"),
        /** payload for the event dispatcher */
        PAYLOAD("pl");

        public final byte[] bytes;
        public final String name;
        // The fields and system fields of records are stored in the same column family : DATA
        public static final byte SYSTEM_PREFIX = (byte)1; // Prefix for the column-qualifiers of system fields
        public static final byte DATA_PREFIX = (byte)2; // Prefix for the column-qualifiers of actual data fields

        RecordColumn(String name) {
            this.name = name;
            this.bytes = Bytes.add(new byte[]{SYSTEM_PREFIX},Bytes.toBytes(name));
        }
    }

}
