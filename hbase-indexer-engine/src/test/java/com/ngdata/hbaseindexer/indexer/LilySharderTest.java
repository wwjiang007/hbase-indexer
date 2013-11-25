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
package com.ngdata.hbaseindexer.indexer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LilySharderTest {

    @Test
    public void testUUIDs() throws IOException, NoSuchAlgorithmException, SharderException {
        doTestSharder("/com/ngdata/hbaseindexer/indexer/uuids.properties");

    }

    @Test
    public void testUserIds() throws IOException, NoSuchAlgorithmException, SharderException {
        doTestSharder("/com/ngdata/hbaseindexer/indexer/userids.properties");

    }

    @Test
    public void testEscapes() throws NoSuchAlgorithmException, SharderException {
        LilySharder lilySharder = new LilySharder(3);

        assertEquals(2, lilySharder.getShard("USER.apple\\.banana\\.pear"));
        assertEquals(0, lilySharder.getShard("USER.apple\\=banana\\=pear"));

    }

    /**
     * tests many ids. Test data is generated using a Lily test run.
     */
    public void doTestSharder(String samples) throws IOException, NoSuchAlgorithmException, SharderException {
        LilySharder lilySharder = new LilySharder(3);

        Properties ids = new Properties();
        ids.load(getClass().getResourceAsStream(samples));

        // basics
        for (String id: ids.stringPropertyNames()) {
            assertEquals(Integer.parseInt(ids.getProperty(id)) - 1, lilySharder.getShard(id));
        }

        // with a simple variant property
        for (String id: ids.stringPropertyNames()) {
            String varId = id.concat(".foo=bar");
            assertEquals(Integer.parseInt(ids.getProperty(id)) - 1, lilySharder.getShard(varId));
        }

        // with a variant property containing an escpaed dot and an escaped equals sign
        for (String id: ids.stringPropertyNames()) {
            String varId = id.concat(".f\\.oo=b\\=ar");
            assertEquals(Integer.parseInt(ids.getProperty(id)) - 1, lilySharder.getShard(varId));
        }

    }

}
