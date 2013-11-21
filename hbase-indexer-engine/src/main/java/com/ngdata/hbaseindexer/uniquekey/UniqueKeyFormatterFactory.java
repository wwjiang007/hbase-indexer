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
package com.ngdata.hbaseindexer.uniquekey;

import com.ngdata.hbaseindexer.conf.IndexerConf;

public class UniqueKeyFormatterFactory {
    private Class formatterClass;

    public UniqueKeyFormatterFactory (IndexerConf conf) {
        this.formatterClass = conf.getUniqueKeyFormatterClass();
    }

    public UniqueKeyFormatter newUniqueKeyFormatter(String tableName) {
        try {
            UniqueKeyFormatter uniqueKeyFormatter = (UniqueKeyFormatter)formatterClass.newInstance();
            if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                ((UniqueTableKeyFormatter) uniqueKeyFormatter).setTable(tableName);
            }
            return uniqueKeyFormatter;
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
    }
}
